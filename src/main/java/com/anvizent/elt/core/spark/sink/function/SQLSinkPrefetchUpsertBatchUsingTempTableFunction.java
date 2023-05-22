package com.anvizent.elt.core.spark.sink.function;

import static com.anvizent.elt.core.spark.constant.FunctionalConstant.TABLE_NAME;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.TaskContext;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.BatchTrackingCountConfigBean;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.constant.Constants;
import com.anvizent.elt.core.lib.constant.StoreType;
import com.anvizent.elt.core.lib.exception.DataCorruptedException;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.InvalidConfigValueException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.lib.function.BaseAnvizentBatchFunction;
import com.anvizent.elt.core.lib.row.formatter.AnvizentErrorSetter;
import com.anvizent.elt.core.lib.row.formatter.BaseAnvizentErrorSetter;
import com.anvizent.elt.core.lib.stats.beans.DBWrittenStats;
import com.anvizent.elt.core.listener.common.connection.RDBMSConnection;
import com.anvizent.elt.core.spark.common.util.SQLUtil;
import com.anvizent.elt.core.spark.constant.Constants.General;
import com.anvizent.elt.core.spark.sink.cache.RecordFromCache;
import com.anvizent.elt.core.spark.sink.cache.SQLSinkCache;
import com.anvizent.elt.core.spark.sink.cache.SQLSinkPrefetchAllCache;
import com.anvizent.elt.core.spark.sink.cache.SQLSinkPrefetchLimitCache;
import com.anvizent.elt.core.spark.sink.config.bean.SQLSinkConfigBean;
import com.anvizent.elt.core.spark.sink.config.bean.SQLUpsertBatchWithTempTableConnections;
import com.anvizent.elt.core.spark.sink.function.bean.SQLSinkPrefetchWithTempTableUpsertFunctionBean;
import com.anvizent.elt.core.spark.sink.service.SQLUpdateService;
import com.anvizent.elt.core.spark.util.StringUtil;

import net.sf.ehcache.CacheManager;

/**
 * @author Hareen Bejjanki
 *
 */
public class SQLSinkPrefetchUpsertBatchUsingTempTableFunction extends BaseAnvizentBatchFunction<HashMap<String, Object>, HashMap<String, Object>> {

	private static final long serialVersionUID = 1L;

	private SQLSinkPrefetchWithTempTableUpsertFunctionBean functionBean;
	private SQLUpsertBatchWithTempTableConnections sqlBatchConnections;
	private static SQLSinkCache sqlSinkCache;

	public SQLSinkPrefetchUpsertBatchUsingTempTableFunction(SQLSinkPrefetchWithTempTableUpsertFunctionBean functionBean, SQLSinkConfigBean sqlSinkConfigBean,
	        LinkedHashMap<String, AnvizentDataType> structure, ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction errorHandlerSinkFunction,
	        JobDetails jobDetails) throws InvalidRelationException, InvalidArgumentsException {
		super(sqlSinkConfigBean, null, structure, structure, null, sqlSinkConfigBean.getMaxRetryCount(), sqlSinkConfigBean.getRetryDelay(),
		        sqlSinkConfigBean.getInitRetryConfigBean().getMaxRetryCount(), sqlSinkConfigBean.getInitRetryConfigBean().getRetryDelay(),
		        sqlSinkConfigBean.getDestroyRetryConfigBean().getMaxRetryCount(), sqlSinkConfigBean.getDestroyRetryConfigBean().getRetryDelay(),
		        sqlSinkConfigBean.getBatchSize(), accumulators, errorHandlerSinkFunction, jobDetails);

		this.functionBean = functionBean;
		this.sqlBatchConnections = new SQLUpsertBatchWithTempTableConnections();
	}

	@Override
	public void onCallStart(Iterator<HashMap<String, Object>> rows, BatchTrackingCountConfigBean trackingCounts) throws ImproperValidationException, Exception {
		try {
			createCache();

			if (sqlBatchConnections.getConnection(TaskContext.getPartitionId()) == null
			        || sqlBatchConnections.getConnection(TaskContext.getPartitionId()).isClosed()) {
				sqlBatchConnections.addConnection(TaskContext.getPartitionId(), getConnection());
				createTempTable();
				SQLUtil.executeQuey(functionBean.getOnConnectRunQuery(), sqlBatchConnections);

				sqlBatchConnections.addInsertStatement(TaskContext.getPartitionId(), getPreparedStatement(functionBean.getInsertQuery()));

				String insertQuery = functionBean.getTempTableInsertQueryInfo().getQuery().replace(TABLE_NAME,
				        '`' + sqlBatchConnections.getTempTableName(TaskContext.getPartitionId()) + '`');
				sqlBatchConnections.addTempTableInsertStatement(TaskContext.getPartitionId(), getPreparedStatement(insertQuery));

				if (functionBean.getDeleteQuery() != null && StringUtils.isNotBlank(functionBean.getDeleteQuery().getQuery())) {
					sqlBatchConnections.addDeleteStatement(TaskContext.getPartitionId(), getPreparedStatement(functionBean.getDeleteQuery().getQuery()));
				}
			} else {
				createTempTable();
				if (sqlBatchConnections.getInsertStatement(TaskContext.getPartitionId()) == null
				        || sqlBatchConnections.getInsertStatement(TaskContext.getPartitionId()).isClosed()) {
					sqlBatchConnections.addInsertStatement(TaskContext.getPartitionId(), getPreparedStatement(functionBean.getInsertQuery()));
				}

				if (sqlBatchConnections.getTempTableInsertStatement(TaskContext.getPartitionId()) == null
				        || sqlBatchConnections.getTempTableInsertStatement(TaskContext.getPartitionId()).isClosed()) {
					String insertQuery = functionBean.getTempTableInsertQueryInfo().getQuery().replace(TABLE_NAME,
					        '`' + sqlBatchConnections.getTempTableName(TaskContext.getPartitionId()) + '`');
					sqlBatchConnections.addTempTableInsertStatement(TaskContext.getPartitionId(), getPreparedStatement(insertQuery));
				}

				if (functionBean.getDeleteQuery() != null && StringUtils.isNotBlank(functionBean.getDeleteQuery().getQuery())
				        && (sqlBatchConnections.getDeleteStatement(TaskContext.getPartitionId()) == null
				                || sqlBatchConnections.getDeleteStatement(TaskContext.getPartitionId()).isClosed())) {
					sqlBatchConnections.addDeleteStatement(TaskContext.getPartitionId(), getPreparedStatement(functionBean.getDeleteQuery().getQuery()));
				}
			}

		} catch (SQLException exception) {
			throw new RecordProcessingException(exception.getMessage(), exception);
		}
	}

	private void createTempTable() throws SQLException {
		if (sqlBatchConnections.getTempTableName(TaskContext.getPartitionId()) == null) {
			String tableName = UUID.randomUUID().toString();

			String query = functionBean.getTempTableCreateQuery().replace(TABLE_NAME, '`' + StringUtil.addMetaChar(tableName, '`', '\\') + '`');
			Statement statement = sqlBatchConnections.getConnection(TaskContext.getPartitionId()).createStatement();
			statement.executeUpdate(query);

			System.out.println("created table: " + tableName);
			sqlBatchConnections.addTempTableNames(TaskContext.getPartitionId(), tableName);
			sqlBatchConnections.addUpdateQueries(TaskContext.getPartitionId(), functionBean.getUpdateQuery().replace(TABLE_NAME, '`' + tableName + '`'));
		}
	}

	private void createCache() throws ImproperValidationException, SQLException {
		CacheManager cacheManager = CacheManager.getInstance();
		String cacheName = functionBean.getName();

		synchronized (SQLSinkPrefetchUpsertBatchFunction.class) {
			if (sqlSinkCache == null && cacheManager.getCache(cacheName) == null) {
				synchronized (SQLSinkPrefetchUpsertBatchFunction.class) {

					System.out.println("SQLSinkPrefetchUpsertBatchFunction.createCache for threadId: " + Thread.currentThread().getId());
					if (functionBean.getPrefetchBatchSize() == null || functionBean.getPrefetchBatchSize() == -1) {
						sqlSinkCache = new SQLSinkPrefetchAllCache(cacheName, functionBean.getRdbmsConnection(), functionBean.getTableName(),
						        functionBean.getKeyFields(), functionBean.getKeyColumns(), functionBean.getSelectColumns(),
						        functionBean.isKeyFieldsCaseSensitive(), functionBean.getMaxElementsInMemory());
					} else {
						sqlSinkCache = new SQLSinkPrefetchLimitCache(cacheName, functionBean.getRdbmsConnection(), functionBean.getTableName(),
						        functionBean.getKeyFields(), functionBean.getKeyColumns(), functionBean.getSelectColumns(), functionBean.getPrefetchBatchSize(),
						        functionBean.isRemoveOnceUsed(), functionBean.isKeyFieldsCaseSensitive(), functionBean.getMaxElementsInMemory());
					}
					cacheManager.addCache(sqlSinkCache);

					if (sqlSinkCache instanceof SQLSinkPrefetchAllCache) {
						try {
							((SQLSinkPrefetchAllCache) sqlSinkCache).fetchData();
						} catch (Exception exception) {
							System.out.println(exception.getMessage());
							throw exception;
						}
					}
				}
			}
		}
	}

	private PreparedStatement getPreparedStatement(String query) throws SQLException {
		return query == null ? null : sqlBatchConnections.getConnection(TaskContext.getPartitionId()).prepareStatement(query);
	}

	private Connection getConnection() throws SQLException, ImproperValidationException {
		RDBMSConnection rdbmsConnection = functionBean.getRdbmsConnection();

		try {
			Class.forName(rdbmsConnection.getDriver());

			String jdbcURL = rdbmsConnection.getJdbcURL();
			if (rdbmsConnection.getDriver().equals(Constants.MYSQL_DRIVER)) {
				jdbcURL = rdbmsConnection.checkAndAddParam(jdbcURL, "rewriteBatchedStatements", General.REWRITE_BATCHED_STATEMENTS, "?", "&");
			}

			Connection connection = DriverManager.getConnection(jdbcURL, rdbmsConnection.getUserName(), rdbmsConnection.getPassword());
			return connection;
		} catch (ClassNotFoundException exception) {
			throw new ImproperValidationException(exception);
		}
	}

	@Override
	public boolean process(HashMap<String, Object> row, ArrayList<HashMap<String, Object>> batchedRows)
	        throws RecordProcessingException, ValidationViolationException, SQLException {
		String deleteField = functionBean.getDeleteIndicatorField();

		if (StringUtils.isNotBlank(deleteField) && row.get(deleteField) != null && (Boolean) row.get(deleteField)) {
			SQLUtil.addPreparedStatementBatch(sqlBatchConnections.getDeleteStatement(TaskContext.getPartitionId()), row, functionBean.getKeyFields(),
			        deleteField);
			return true;
		} else {
			return processAfterDelete(row, batchedRows);
		}
	}

	private boolean processAfterDelete(HashMap<String, Object> row, ArrayList<HashMap<String, Object>> batchedRows)
	        throws RecordProcessingException, ValidationViolationException {
		HashMap<String, Object> processValues = null;
		try {
			processValues = recordExists(row);
			boolean recordExists = (boolean) processValues.get(General.RECORD_EXISTS);
			RecordFromCache recordFromCache = (RecordFromCache) processValues.get(General.RECORD_FROM_CACHE);
			boolean doUpdate = (boolean) processValues.get(General.DO_UPDATE);

			boolean result = upsert(row, recordFromCache.isExtracted(), recordExists, doUpdate);
			recordFromCache.setExtracted(true);

			return result;
		} catch (SQLException exception) {
			throw new RecordProcessingException(exception.getMessage(), exception);
		} catch (InvalidConfigValueException exception) {
			throw new ValidationViolationException(exception.getMessage(), exception);
		}
	}

	private boolean upsert(HashMap<String, Object> row, boolean recordProcessed, boolean recordExists, boolean doUpdate) throws SQLException {
		if (recordProcessed) {
			return false;
		} else if (!recordExists) {
			addInsertBatch(row);
			return true;
		} else if (recordExists && doUpdate) {
			addTempTableInsertBatch(row);
			return true;
		} else {
			return false;
		}
	}

	private void addTempTableInsertBatch(HashMap<String, Object> row) throws SQLException {
		SQLUtil.setPreparedStatement(sqlBatchConnections.getTempTableInsertStatement(TaskContext.getPartitionId()), row, functionBean.getRowKeysAndFields());
		sqlBatchConnections.getTempTableInsertStatement(TaskContext.getPartitionId()).addBatch();
	}

	private void addInsertBatch(HashMap<String, Object> row) throws SQLException {
		SQLUtil.setPreparedStatement(sqlBatchConnections.getInsertStatement(TaskContext.getPartitionId()), row, functionBean.getRowKeysAndFields());
		sqlBatchConnections.getInsertStatement(TaskContext.getPartitionId()).addBatch();
	}

	private HashMap<String, Object> recordExists(HashMap<String, Object> row) throws SQLException, RecordProcessingException, InvalidConfigValueException {
		HashMap<String, Object> processValues = new HashMap<>();

		RecordFromCache recordFromCache = sqlSinkCache.getSelectValues(row);
		processValues.put(General.RECORD_EXISTS, recordFromCache.isFound());
		processValues.put(General.RECORD_FROM_CACHE, recordFromCache);

		if (recordFromCache.isFound() && !recordFromCache.isExtracted()) {
			processValues.put(General.DO_UPDATE,
			        SQLUpdateService.checkForUpdate(row, recordFromCache.getValue(), functionBean.isAlwaysUpdate(), functionBean.getSelectFields()));
		} else {
			processValues.put(General.DO_UPDATE, false);
		}

		return processValues;
	}

	@Override
	public void beforeBatch(Iterator<HashMap<String, Object>> rows) throws SQLException {
	}

	@Override
	public DBWrittenStats onBatch(Iterator<HashMap<String, Object>> rows, long batchNumber, long currentBatchCount,
	        ArrayList<HashMap<String, Object>> batchedRows) throws RecordProcessingException, DataCorruptedException {
		System.out.println("\n\n\n\nOn Batch: batchNumber: " + batchNumber + "before");
		Statement updateStatement = null;
		try {
			updateStatement = sqlBatchConnections.getConnection(TaskContext.getPartitionId()).createStatement();
			if (StringUtils.isNotBlank(functionBean.getDeleteIndicatorField())) {
				long deletedCount = sqlBatchConnections.getDeleteStatement(TaskContext.getPartitionId()) != null
				        ? sqlBatchConnections.getDeleteStatement(TaskContext.getPartitionId()).executeBatch().length
				        : 0;
				System.out.println("Delete count: " + deletedCount);
			}

			Long before = new Date().getTime();
			System.out.println("\n\n\n\nOn Batch: batchNumber: " + batchNumber + ", before insert");
			long insertedCount = sqlBatchConnections.getInsertStatement(TaskContext.getPartitionId()) != null
			        ? sqlBatchConnections.getInsertStatement(TaskContext.getPartitionId()).executeBatch().length
			        : 0;
			System.out.println("\n\n\n\nOn Batch: batchNumber: " + batchNumber + ", after insert, time taken: " + +(new Date().getTime() - before)
			        + ", insertedCount: " + insertedCount);

			before = new Date().getTime();
			System.out.println("\n\n\n\nOn Batch: batchNumber: " + batchNumber + ", before tempTableInsert");
			long tempTableInsertCount = sqlBatchConnections.getTempTableInsertStatement(TaskContext.getPartitionId()) != null
			        ? sqlBatchConnections.getTempTableInsertStatement(TaskContext.getPartitionId()).executeBatch().length
			        : 0;
			System.out.println("\n\n\n\nOn Batch: batchNumber: " + batchNumber + ", after tempTableInsert, time taken: " + (new Date().getTime() - before)
			        + ", tempTableInsertCount: " + tempTableInsertCount);

			before = new Date().getTime();
			System.out.println("\n\n\n\nOn Batch: batchNumber: " + batchNumber + ", before update");
			long updatedCount = updateStatement.executeUpdate(sqlBatchConnections.getUpdateQuery(TaskContext.getPartitionId()));
			System.out.println("\n\n\n\nOn Batch: batchNumber: " + batchNumber + ", after update, time taken: " + (new Date().getTime() - before)
			        + ", updatedCount: " + updatedCount);

			return new DBWrittenStats(insertedCount, updatedCount);
		} catch (SQLException sqlException) {
			if (functionBean.getStoreType() != null && StoreType.DATA_CORRUPTED_ERROR_CODES.get(functionBean.getStoreType()) != null
			        && StoreType.DATA_CORRUPTED_ERROR_CODES.get(functionBean.getStoreType()).contains(sqlException.getErrorCode())) {
				throw new DataCorruptedException(sqlException.getMessage(), sqlException);
			} else {
				throw new RecordProcessingException(sqlException.getMessage(), sqlException);
			}
		} finally {
			if (updateStatement != null) {
				try {
					updateStatement.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	public void afterBatch(Iterator<HashMap<String, Object>> rows, ArrayList<HashMap<String, Object>> batchedRows) throws Exception {
		try {
			if (sqlBatchConnections.getInsertStatement(TaskContext.getPartitionId()) != null) {
				sqlBatchConnections.getInsertStatement(TaskContext.getPartitionId()).clearBatch();
			}

			if (sqlBatchConnections.getTempTableInsertStatement(TaskContext.getPartitionId()) != null) {
				sqlBatchConnections.getTempTableInsertStatement(TaskContext.getPartitionId()).clearBatch();
			}

			if (StringUtils.isNotBlank(functionBean.getDeleteIndicatorField())
			        && sqlBatchConnections.getDeleteStatement(TaskContext.getPartitionId()) != null) {
				sqlBatchConnections.getDeleteStatement(TaskContext.getPartitionId()).clearBatch();
			}
		} catch (SQLException exception) {
			throw new RecordProcessingException(exception.getMessage(), exception);
		}
	}

	@Override
	public void onCallEnd(Iterator<HashMap<String, Object>> rows, BatchTrackingCountConfigBean trackingCounts) throws Exception {
		try {
			if (sqlBatchConnections.getInsertStatement(TaskContext.getPartitionId()) != null
			        && !sqlBatchConnections.getInsertStatement(TaskContext.getPartitionId()).isClosed()) {
				sqlBatchConnections.getInsertStatement(TaskContext.getPartitionId()).close();
			}

			if (sqlBatchConnections.getTempTableInsertStatement(TaskContext.getPartitionId()) != null
			        && !sqlBatchConnections.getTempTableInsertStatement(TaskContext.getPartitionId()).isClosed()) {
				sqlBatchConnections.getTempTableInsertStatement(TaskContext.getPartitionId()).close();
			}

			if (StringUtils.isNotBlank(functionBean.getDeleteIndicatorField()) && sqlBatchConnections.getDeleteStatement(TaskContext.getPartitionId()) != null
			        && !sqlBatchConnections.getDeleteStatement(TaskContext.getPartitionId()).isClosed()) {
				sqlBatchConnections.getDeleteStatement(TaskContext.getPartitionId()).close();
			}

			dropTempTable();

			if (sqlBatchConnections.getConnection(TaskContext.getPartitionId()) != null
			        && !sqlBatchConnections.getConnection(TaskContext.getPartitionId()).isClosed()) {
				sqlBatchConnections.getConnection(TaskContext.getPartitionId()).close();
			}
		} catch (SQLException exception) {
			throw new RecordProcessingException(exception.getMessage(), exception);
		}
	}

	private void dropTempTable() throws SQLException {
		String tableName = sqlBatchConnections.getTempTableName(TaskContext.getPartitionId());
		if (StringUtils.isNoneBlank(tableName)) {
			String query = "DROP TABLE `" + tableName + "`";
			Statement statement = sqlBatchConnections.getConnection(TaskContext.getPartitionId()).createStatement();
			statement.executeUpdate(query);
			System.out.println("Dropped table " + tableName);

			sqlBatchConnections.removeTempTableNames(TaskContext.getPartitionId());
		}
	}

	@Override
	public void onBatchSuccess(long batchNumber, long currentBatchCount, ArrayList<HashMap<String, Object>> batchedRows) throws Exception {
	}

	@Override
	public void onBatchFail(long batchNumber, long currentBatchCount, ArrayList<HashMap<String, Object>> batchedRows) throws Exception {
	}

	@Override
	protected BaseAnvizentErrorSetter<HashMap<String, Object>, HashMap<String, Object>> getBaseAnvizentErrorRowSetter() throws InvalidArgumentsException {
		return AnvizentErrorSetter.getDefaultInstance();
	}
}
