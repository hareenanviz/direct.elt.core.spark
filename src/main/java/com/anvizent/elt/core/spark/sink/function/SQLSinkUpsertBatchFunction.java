package com.anvizent.elt.core.spark.sink.function;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;

import org.apache.commons.collections.CollectionUtils;
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
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.lib.function.BaseAnvizentBatchFunction;
import com.anvizent.elt.core.lib.row.formatter.AnvizentErrorSetter;
import com.anvizent.elt.core.lib.row.formatter.BaseAnvizentErrorSetter;
import com.anvizent.elt.core.lib.stats.beans.DBWrittenStats;
import com.anvizent.elt.core.listener.common.connection.RDBMSConnection;
import com.anvizent.elt.core.spark.common.util.SQLUtil;
import com.anvizent.elt.core.spark.constant.Constants.General;
import com.anvizent.elt.core.spark.constant.DBInsertMode;
import com.anvizent.elt.core.spark.sink.cache.RecordFromCache;
import com.anvizent.elt.core.spark.sink.cache.SQLSinkCache;
import com.anvizent.elt.core.spark.sink.cache.SQLSinkPrefetchAllCache;
import com.anvizent.elt.core.spark.sink.cache.SQLSinkPrefetchLimitCache;
import com.anvizent.elt.core.spark.sink.config.bean.SQLSinkConfigBean;
import com.anvizent.elt.core.spark.sink.config.bean.SQLUpsertBatchConnections;
import com.anvizent.elt.core.spark.sink.service.SQLUpdateService;

import net.sf.ehcache.CacheManager;

/**
 * @author Hareen Bejjanki
 *
 */
public class SQLSinkUpsertBatchFunction extends BaseAnvizentBatchFunction<HashMap<String, Object>, HashMap<String, Object>> {

	private static final long serialVersionUID = 1L;

	private SQLSinkConfigBean sqlSinkConfigBean;
	private RDBMSConnection rdbmsConnection;
	private SQLUpsertBatchConnections sqlBatchConnections;
	private SQLSinkCache sqlSinkCache;

	public SQLSinkUpsertBatchFunction(SQLSinkConfigBean sqlSinkConfigBean, LinkedHashMap<String, AnvizentDataType> structure,
	        ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails)
	        throws InvalidRelationException, InvalidArgumentsException {
		super(sqlSinkConfigBean, null, structure, structure, null, sqlSinkConfigBean.getMaxRetryCount(), sqlSinkConfigBean.getRetryDelay(),
		        sqlSinkConfigBean.getInitRetryConfigBean().getMaxRetryCount(), sqlSinkConfigBean.getInitRetryConfigBean().getRetryDelay(),
		        sqlSinkConfigBean.getDestroyRetryConfigBean().getMaxRetryCount(), sqlSinkConfigBean.getDestroyRetryConfigBean().getRetryDelay(),
		        sqlSinkConfigBean.getBatchSize(), accumulators, errorHandlerSinkFunction, jobDetails);

		this.sqlSinkConfigBean = sqlSinkConfigBean;
		this.rdbmsConnection = sqlSinkConfigBean.getRdbmsConnection();
		this.sqlBatchConnections = new SQLUpsertBatchConnections();

	}

	@Override
	public void onCallStart(Iterator<HashMap<String, Object>> rows, BatchTrackingCountConfigBean trackingCounts) throws ImproperValidationException, Exception {
		try {
			createCache();

			if (sqlBatchConnections.getConnection(TaskContext.getPartitionId()) == null
			        || sqlBatchConnections.getConnection(TaskContext.getPartitionId()).isClosed()) {
				sqlBatchConnections.addConnection(TaskContext.getPartitionId(), getConnection());
				SQLUtil.executeQuey(sqlSinkConfigBean.getOnConnectRunQuery(), sqlBatchConnections);

				sqlBatchConnections.addInsertStatement(TaskContext.getPartitionId(), getPreparedStatement(sqlSinkConfigBean.getInsertQuery()));
				sqlBatchConnections.addUpdateStatement(TaskContext.getPartitionId(), getPreparedStatement(sqlSinkConfigBean.getUpdateQuery()));

				if (StringUtils.isNotBlank(sqlSinkConfigBean.getDeleteQuery())) {
					sqlBatchConnections.addDeleteStatement(TaskContext.getPartitionId(), getPreparedStatement(sqlSinkConfigBean.getDeleteQuery()));
				}

				sqlBatchConnections.getConnection(TaskContext.getPartitionId()).setAutoCommit(Boolean.FALSE);
			} else {
				if (sqlBatchConnections.getInsertStatement(TaskContext.getPartitionId()) == null
				        || sqlBatchConnections.getInsertStatement(TaskContext.getPartitionId()).isClosed()) {
					sqlBatchConnections.addInsertStatement(TaskContext.getPartitionId(), getPreparedStatement(sqlSinkConfigBean.getInsertQuery()));
				}

				if (sqlBatchConnections.getUpdateStatement(TaskContext.getPartitionId()) == null
				        || sqlBatchConnections.getUpdateStatement(TaskContext.getPartitionId()).isClosed()) {
					sqlBatchConnections.addUpdateStatement(TaskContext.getPartitionId(), getPreparedStatement(sqlSinkConfigBean.getUpdateQuery()));
				}

				if (StringUtils.isNotBlank(sqlSinkConfigBean.getDeleteQuery()) && (sqlBatchConnections.getDeleteStatement(TaskContext.getPartitionId()) == null
				        || sqlBatchConnections.getDeleteStatement(TaskContext.getPartitionId()).isClosed())) {
					sqlBatchConnections.addDeleteStatement(TaskContext.getPartitionId(), getPreparedStatement(sqlSinkConfigBean.getDeleteQuery()));
				}
			}
		} catch (SQLException exception) {
			throw new RecordProcessingException(exception.getMessage(), exception);
		}
	}

	private synchronized void createCache() throws ImproperValidationException, SQLException {
		CacheManager cacheManager = CacheManager.getInstance();
		String cacheName = TaskContext.getPartitionId() + "_" + sqlSinkConfigBean.getName();
		ArrayList<String> keyColumns = sqlSinkConfigBean.getKeyColumns();
		if (CollectionUtils.isEmpty(keyColumns)) {
			keyColumns = sqlSinkConfigBean.getKeyFields();
		}

		if (this.sqlSinkCache == null && cacheManager.getCache(cacheName) == null) {
			if (sqlSinkConfigBean.getPrefetchBatchSize() == null || sqlSinkConfigBean.getPrefetchBatchSize() == -1) {
				this.sqlSinkCache = new SQLSinkPrefetchAllCache(cacheName, sqlSinkConfigBean.getRdbmsConnection(), sqlSinkConfigBean.getTableName(),
				        sqlSinkConfigBean.getKeyFields(), keyColumns, sqlSinkConfigBean.getSelectColumns(), sqlSinkConfigBean.isKeyFieldsCaseSensitive(),
				        sqlSinkConfigBean.getMaxElementsInMemory());
			} else {
				this.sqlSinkCache = new SQLSinkPrefetchLimitCache(cacheName, sqlSinkConfigBean.getRdbmsConnection(), sqlSinkConfigBean.getTableName(),
				        sqlSinkConfigBean.getKeyFields(), keyColumns, sqlSinkConfigBean.getSelectColumns(), sqlSinkConfigBean.getPrefetchBatchSize(),
				        sqlSinkConfigBean.isRemoveOnceUsed(), sqlSinkConfigBean.isKeyFieldsCaseSensitive(), sqlSinkConfigBean.getMaxElementsInMemory());
			}
			cacheManager.addCache(this.sqlSinkCache);
		}
	}

	private PreparedStatement getPreparedStatement(String query) throws SQLException {
		return query == null ? null : sqlBatchConnections.getConnection(TaskContext.getPartitionId()).prepareStatement(query);
	}

	private Connection getConnection() throws SQLException, ImproperValidationException {
		try {
			Class.forName(rdbmsConnection.getDriver());

			String jdbcURL = rdbmsConnection.getJdbcURL();
			if (rdbmsConnection.getDriver().equals(Constants.MYSQL_DRIVER)) {
				jdbcURL = rdbmsConnection.checkAndAddParam(jdbcURL, "rewriteBatchedStatements", General.REWRITE_BATCHED_STATEMENTS, "?", "&");
			}

			return DriverManager.getConnection(jdbcURL, rdbmsConnection.getUserName(), rdbmsConnection.getPassword());
		} catch (ClassNotFoundException exception) {
			throw new ImproperValidationException(exception);
		}
	}

	@Override
	public boolean process(HashMap<String, Object> row, ArrayList<HashMap<String, Object>> batchedRows)
	        throws RecordProcessingException, ValidationViolationException, SQLException {
		String deleteField = sqlSinkConfigBean.getDeleteIndicatorField();

		if (StringUtils.isNotBlank(deleteField) && row.get(deleteField) != null && (Boolean) row.get(deleteField)) {
			SQLUtil.addPreparedStatementBatch(sqlBatchConnections.getDeleteStatement(TaskContext.getPartitionId()), row, sqlSinkConfigBean.getKeyFields(),
			        sqlSinkConfigBean.getDeleteIndicatorField());
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
			boolean doUpdate = (boolean) processValues.get(General.DO_UPDATE);

			if (sqlSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPSERT)) {
				return upsert(row, recordExists, doUpdate);
			} else {
				throw new UnimplementedException();
			}
		} catch (SQLException exception) {
			throw new RecordProcessingException(exception.getMessage(), exception);
		} catch (UnimplementedException | InvalidConfigValueException exception) {
			throw new ValidationViolationException(exception.getMessage(), exception);
		}
	}

	private boolean upsert(HashMap<String, Object> row, boolean recordExists, boolean doUpdate) throws SQLException {
		if (!recordExists) {
			addInsertBatch(row);
			return true;
		} else if (recordExists && doUpdate) {
			addUpdateBatch(row);
			return true;
		} else {
			return false;
		}
	}

	private void addUpdateBatch(HashMap<String, Object> row) throws SQLException {
		SQLUtil.setPreparedStatement(sqlBatchConnections.getUpdateStatement(TaskContext.getPartitionId()), row, sqlSinkConfigBean.getRowFields());
		SQLUtil.setPreparedStatement(sqlBatchConnections.getUpdateStatement(TaskContext.getPartitionId()), row, sqlSinkConfigBean.getKeyFields(),
		        sqlSinkConfigBean.getRowFields().size() + 1);
		sqlBatchConnections.getUpdateStatement(TaskContext.getPartitionId()).addBatch();
	}

	private void addInsertBatch(HashMap<String, Object> row) throws SQLException {
		SQLUtil.setPreparedStatement(sqlBatchConnections.getInsertStatement(TaskContext.getPartitionId()), row, sqlSinkConfigBean.getRowKeysAndFields());
		sqlBatchConnections.getInsertStatement(TaskContext.getPartitionId()).addBatch();
	}

	private HashMap<String, Object> recordExists(HashMap<String, Object> row) throws SQLException, RecordProcessingException, InvalidConfigValueException {
		HashMap<String, Object> processValues = new HashMap<>();

		RecordFromCache recordFromCache = sqlSinkCache.getSelectValues(row);
		processValues.put(General.RECORD_EXISTS, recordFromCache.isFound());

		if (recordFromCache.isFound()) {
			processValues.put(General.DO_UPDATE, SQLUpdateService.checkForUpdate(row, recordFromCache.getValue(), sqlSinkConfigBean));
		} else {
			processValues.put(General.DO_UPDATE, false);
		}

		return processValues;
	}

	@Override
	public void beforeBatch(Iterator<HashMap<String, Object>> rows) {
	}

	@Override
	public DBWrittenStats onBatch(Iterator<HashMap<String, Object>> rows, long batchNumber, long currentBatchCount,
	        ArrayList<HashMap<String, Object>> batchedRows) throws RecordProcessingException, DataCorruptedException {
		try {
			if (StringUtils.isNotBlank(sqlSinkConfigBean.getDeleteIndicatorField())) {
				long deletedCount = sqlBatchConnections.getDeleteStatement(TaskContext.getPartitionId()) != null
				        ? sqlBatchConnections.getDeleteStatement(TaskContext.getPartitionId()).executeBatch().length
				        : 0;
				System.out.println("Delete count: " + deletedCount);
			}

			long insertedCount = sqlBatchConnections.getInsertStatement(TaskContext.getPartitionId()) != null
			        ? sqlBatchConnections.getInsertStatement(TaskContext.getPartitionId()).executeBatch().length
			        : 0;
			long updatedCount = sqlBatchConnections.getUpdateStatement(TaskContext.getPartitionId()) != null
			        ? sqlBatchConnections.getUpdateStatement(TaskContext.getPartitionId()).executeBatch().length
			        : 0;

			return new DBWrittenStats(insertedCount, updatedCount);
		} catch (SQLException sqlException) {
			StoreType storeType = StoreType.getInstance(sqlSinkConfigBean.getRdbmsConnection().getDriver());
			if (storeType != null && StoreType.DATA_CORRUPTED_ERROR_CODES.get(storeType) != null
			        && StoreType.DATA_CORRUPTED_ERROR_CODES.get(storeType).contains(sqlException.getErrorCode())) {
				throw new DataCorruptedException(sqlException.getMessage(), sqlException);
			} else {
				throw new RecordProcessingException(sqlException.getMessage(), sqlException);
			}
		}
	}

	@Override
	public void afterBatch(Iterator<HashMap<String, Object>> rows, ArrayList<HashMap<String, Object>> batchedRows) throws Exception {
		try {
			if (sqlBatchConnections.getInsertStatement(TaskContext.getPartitionId()) != null) {
				sqlBatchConnections.getInsertStatement(TaskContext.getPartitionId()).clearBatch();
			}

			if (sqlBatchConnections.getUpdateStatement(TaskContext.getPartitionId()) != null) {
				sqlBatchConnections.getUpdateStatement(TaskContext.getPartitionId()).clearBatch();
			}

			if (StringUtils.isNotBlank(sqlSinkConfigBean.getDeleteIndicatorField())
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

			if (sqlBatchConnections.getUpdateStatement(TaskContext.getPartitionId()) != null
			        && !sqlBatchConnections.getUpdateStatement(TaskContext.getPartitionId()).isClosed()) {
				sqlBatchConnections.getUpdateStatement(TaskContext.getPartitionId()).close();
			}

			if (StringUtils.isNotBlank(sqlSinkConfigBean.getDeleteIndicatorField())
			        && sqlBatchConnections.getDeleteStatement(TaskContext.getPartitionId()) != null
			        && !sqlBatchConnections.getDeleteStatement(TaskContext.getPartitionId()).isClosed()) {
				sqlBatchConnections.getDeleteStatement(TaskContext.getPartitionId()).close();
			}

			if (sqlBatchConnections.getConnection(TaskContext.getPartitionId()) != null
			        && !sqlBatchConnections.getConnection(TaskContext.getPartitionId()).isClosed()) {
				sqlBatchConnections.getConnection(TaskContext.getPartitionId()).close();
			}
		} catch (SQLException exception) {
			throw new RecordProcessingException(exception.getMessage(), exception);
		}
	}

	@Override
	public void onBatchSuccess(long batchNumber, long currentBatchCount, ArrayList<HashMap<String, Object>> batchedRows) throws Exception {
		try {
			if (sqlBatchConnections.getConnection(TaskContext.getPartitionId()) != null) {
				sqlBatchConnections.getConnection(TaskContext.getPartitionId()).commit();
			}
		} catch (SQLException exception) {
			throw new RecordProcessingException(exception.getMessage(), exception);
		}
	}

	@Override
	public void onBatchFail(long batchNumber, long currentBatchCount, ArrayList<HashMap<String, Object>> batchedRows) throws Exception {
		try {
			if (sqlBatchConnections.getConnection(TaskContext.getPartitionId()) != null) {
				sqlBatchConnections.getConnection(TaskContext.getPartitionId()).rollback();
			}

			onCallStart(null, null);
		} catch (SQLException exception) {
			throw new RecordProcessingException(exception.getMessage(), exception);
		}
	}

	@Override
	protected BaseAnvizentErrorSetter<HashMap<String, Object>, HashMap<String, Object>> getBaseAnvizentErrorRowSetter() throws InvalidArgumentsException {
		return AnvizentErrorSetter.getDefaultInstance();
	}
}
