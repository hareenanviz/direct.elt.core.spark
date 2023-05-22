package com.anvizent.elt.core.spark.sink.function;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;

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
import com.anvizent.elt.core.spark.exception.RecordAlreadyExistsException;
import com.anvizent.elt.core.spark.exception.RecordDoesNotExistsForUpdateException;
import com.anvizent.elt.core.spark.sink.config.bean.SQLBatchConnections;
import com.anvizent.elt.core.spark.sink.config.bean.SQLSinkConfigBean;
import com.anvizent.elt.core.spark.sink.service.SQLUpdateService;

/**
 * @author Hareen Bejjanki
 *
 */
public class SQLSinkBatchFunction extends BaseAnvizentBatchFunction<HashMap<String, Object>, HashMap<String, Object>> {

	private static final long serialVersionUID = 1L;

	private SQLSinkConfigBean sqlSinkConfigBean;
	private RDBMSConnection rdbmsConnection;
	private SQLBatchConnections sqlBatchConnections;
	private ArrayList<ArrayList<Object>> keysAdded = new ArrayList<ArrayList<Object>>();

	public SQLSinkBatchFunction(SQLSinkConfigBean sqlSinkConfigBean, LinkedHashMap<String, AnvizentDataType> structure,
	        ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails)
	        throws InvalidRelationException, InvalidArgumentsException {
		super(sqlSinkConfigBean, null, structure, structure, null, sqlSinkConfigBean.getMaxRetryCount(), sqlSinkConfigBean.getRetryDelay(),
		        sqlSinkConfigBean.getInitRetryConfigBean().getMaxRetryCount(), sqlSinkConfigBean.getInitRetryConfigBean().getRetryDelay(),
		        sqlSinkConfigBean.getDestroyRetryConfigBean().getMaxRetryCount(), sqlSinkConfigBean.getDestroyRetryConfigBean().getRetryDelay(),
		        sqlSinkConfigBean.getBatchSize(), accumulators, errorHandlerSinkFunction, jobDetails);

		this.sqlSinkConfigBean = sqlSinkConfigBean;
		this.rdbmsConnection = sqlSinkConfigBean.getRdbmsConnection();
		this.sqlBatchConnections = new SQLBatchConnections();
	}

	@Override
	public void onCallStart(Iterator<HashMap<String, Object>> rows, BatchTrackingCountConfigBean trackingCounts) throws ImproperValidationException, Exception {
		try {
			if (sqlBatchConnections.getConnection(TaskContext.getPartitionId()) == null
			        || sqlBatchConnections.getConnection(TaskContext.getPartitionId()).isClosed()) {
				sqlBatchConnections.addConnection(TaskContext.getPartitionId(), getConnection());

				sqlBatchConnections.addSelectStatement(TaskContext.getPartitionId(), getPreparedStatement(sqlSinkConfigBean.getSelectQuery()));
				sqlBatchConnections.addInsertStatement(TaskContext.getPartitionId(), getPreparedStatement(sqlSinkConfigBean.getInsertQuery()));
				sqlBatchConnections.addUpdateStatement(TaskContext.getPartitionId(), getPreparedStatement(sqlSinkConfigBean.getUpdateQuery()));
				sqlBatchConnections.addUpsertStatement(TaskContext.getPartitionId(),
				        sqlSinkConfigBean.getUpsertQueryInfo() == null ? null : getPreparedStatement(sqlSinkConfigBean.getUpsertQueryInfo().getQuery()));

				sqlBatchConnections.getConnection(TaskContext.getPartitionId()).setAutoCommit(Boolean.FALSE);
			}

			if (sqlBatchConnections.getSelectStatement(TaskContext.getPartitionId()) == null
			        || sqlBatchConnections.getSelectStatement(TaskContext.getPartitionId()).isClosed()) {
				sqlBatchConnections.addSelectStatement(TaskContext.getPartitionId(), getPreparedStatement(sqlSinkConfigBean.getSelectQuery()));
			}

			if (sqlBatchConnections.getInsertStatement(TaskContext.getPartitionId()) == null
			        || sqlBatchConnections.getInsertStatement(TaskContext.getPartitionId()).isClosed()) {
				sqlBatchConnections.addInsertStatement(TaskContext.getPartitionId(), getPreparedStatement(sqlSinkConfigBean.getInsertQuery()));
			}

			if (sqlBatchConnections.getUpdateStatement(TaskContext.getPartitionId()) == null
			        || sqlBatchConnections.getUpdateStatement(TaskContext.getPartitionId()).isClosed()) {
				sqlBatchConnections.addUpdateStatement(TaskContext.getPartitionId(), getPreparedStatement(sqlSinkConfigBean.getUpdateQuery()));
			}

			if (sqlBatchConnections.getUpsertStatement(TaskContext.getPartitionId()) == null
			        || sqlBatchConnections.getUpsertStatement(TaskContext.getPartitionId()).isClosed()) {
				sqlBatchConnections.addUpsertStatement(TaskContext.getPartitionId(),
				        getPreparedStatement(sqlSinkConfigBean.getUpsertQueryInfo() == null ? null : sqlSinkConfigBean.getUpsertQueryInfo().getQuery()));
			}
		} catch (SQLException exception) {
			throw new RecordProcessingException(exception.getMessage(), exception);
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

	@SuppressWarnings("unchecked")
	@Override
	public boolean process(HashMap<String, Object> row, ArrayList<HashMap<String, Object>> batchedRows)
	        throws RecordProcessingException, ValidationViolationException {
		HashMap<String, Object> processValues = null;
		try {
			if (sqlSinkConfigBean.getDBInsertMode().equals(DBInsertMode.INSERT)
			        && (sqlSinkConfigBean.getKeyFields() == null || sqlSinkConfigBean.getKeyFields().isEmpty())) {
				return insert(row, false);
			} else {
				processValues = recordExists(row);
				boolean recordExists = (boolean) processValues.get(General.RECORD_EXISTS);
				boolean doUpdate = (boolean) processValues.get(General.DO_UPDATE);

				if (sqlSinkConfigBean.getDBInsertMode().equals(DBInsertMode.INSERT)) {
					return insert(row, recordExists);
				} else if (sqlSinkConfigBean.getDBInsertMode().equals(DBInsertMode.INSERT_IF_NOT_EXISTS)) {
					return insertIfNotExists(row, recordExists);
				} else if (sqlSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPDATE)) {
					return update(row, recordExists, doUpdate);
				} else if (sqlSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPSERT)) {
					return upsert(row);
				} else {
					throw new UnimplementedException();
				}
			}
		} catch (SQLException exception) {
			if (processValues != null) {
				ArrayList<Object> keys = (ArrayList<Object>) processValues.get(General.KEYS);
				keysAdded.remove(keys);
			}

			throw new RecordProcessingException(exception.getMessage(), exception);
		} catch (RecordAlreadyExistsException | RecordDoesNotExistsForUpdateException exception) {
			if (processValues != null) {
				ArrayList<Object> keys = (ArrayList<Object>) processValues.get(General.KEYS);
				keysAdded.remove(keys);
			}

			throw new RecordProcessingException(exception.getMessage(), new DataCorruptedException(exception));
		} catch (UnimplementedException | InvalidConfigValueException exception) {
			if (processValues != null) {
				ArrayList<Object> keys = (ArrayList<Object>) processValues.get(General.KEYS);
				keysAdded.remove(keys);
			}

			throw new ValidationViolationException(exception.getMessage(), exception);
		}
	}

	private boolean insert(HashMap<String, Object> row, boolean recordExists) throws SQLException, RecordAlreadyExistsException {
		if (!recordExists) {
			addInsertBatch(row);
			return true;
		} else {
			throw new RecordAlreadyExistsException("Record already exists.");
		}
	}

	private boolean insertIfNotExists(HashMap<String, Object> row, boolean recordExists) throws SQLException {
		if (!recordExists) {
			addInsertBatch(row);
			return true;
		} else {
			return false;
		}
	}

	private boolean update(HashMap<String, Object> row, boolean recordExists, boolean doUpdate) throws SQLException, RecordDoesNotExistsForUpdateException {
		if (recordExists && doUpdate) {
			addUpdateBatch(row);
			return true;
		} else if (!recordExists) {
			throw new RecordDoesNotExistsForUpdateException("Record does not exits in '" + sqlSinkConfigBean.getTableName() + "' target table for update.");
		} else {
			return false;
		}
	}

	private boolean upsert(HashMap<String, Object> row) throws SQLException {
		SQLUtil.setPreparedStatement(sqlBatchConnections.getUpsertStatement(TaskContext.getPartitionId()), row, sqlSinkConfigBean.getRowKeysAndFields(),
		        sqlSinkConfigBean.getUpsertQueryInfo().getMultiValuedFieldIndexesToSet());
		sqlBatchConnections.getUpsertStatement(TaskContext.getPartitionId()).addBatch();

		return true;
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
		processValues.put(General.RECORD_EXISTS, Boolean.FALSE);
		processValues.put(General.DO_UPDATE, Boolean.FALSE);

		if (sqlSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPSERT)) {
			return processValues;
		}

		ArrayList<Object> keys = new ArrayList<Object>();

		for (int i = 0; i < sqlSinkConfigBean.getKeyFields().size(); i++) {
			if (rdbmsConnection.getDriver().equals(Constants.MYSQL_DRIVER) && !sqlSinkConfigBean.isKeyFieldsCaseSensitive()
			        && row.get(sqlSinkConfigBean.getKeyFields().get(i)).getClass().equals(String.class)) {
				keys.add(((String) row.get(sqlSinkConfigBean.getKeyFields().get(i))).toUpperCase());
			} else {
				keys.add(row.get(sqlSinkConfigBean.getKeyFields().get(i)));
			}
		}

		processValues.put(General.KEYS, keys);
		if (keysAdded.contains(keys)) {
			processValues.put(General.RECORD_EXISTS, Boolean.TRUE);
			processValues.put(General.DO_UPDATE, Boolean.TRUE);
		} else {
			recordExistsInDB(row, keys, processValues);
			keysAdded.add(keys);
		}

		return processValues;
	}

	private void recordExistsInDB(HashMap<String, Object> row, ArrayList<Object> keys, HashMap<String, Object> processValues)
	        throws SQLException, RecordProcessingException, InvalidConfigValueException {
		setPreparedStatement(sqlBatchConnections.getSelectStatement(TaskContext.getPartitionId()), keys);
		ResultSet resultSet = sqlBatchConnections.getSelectStatement(TaskContext.getPartitionId()).executeQuery();

		if (resultSet != null && resultSet.next()) {
			processValues.put(General.RECORD_EXISTS, Boolean.TRUE);

			if (sqlSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPDATE) || sqlSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPSERT)) {
				processValues.put(General.DO_UPDATE,
				        SQLUpdateService.checkForUpdate(resultSet, row, sqlSinkConfigBean.isAlwaysUpdate(), sqlSinkConfigBean.getChecksumField(),
				                sqlSinkConfigBean.getMetaDataFields(), sqlSinkConfigBean.getRowFields(), sqlSinkConfigBean.getFieldsDifferToColumns(),
				                sqlSinkConfigBean.getColumnsDifferToFields()));
			}
		}

		SQLUtil.closeResultSetObject(resultSet);
	}

	private void setPreparedStatement(PreparedStatement preparedStatement, ArrayList<Object> keys) throws SQLException {
		for (int i = 0; i < keys.size(); i++) {
			sqlBatchConnections.getSelectStatement(TaskContext.getPartitionId()).setObject(i + 1, keys.get(i));
		}
	}

	@Override
	public void beforeBatch(Iterator<HashMap<String, Object>> rows) {
		keysAdded.clear();
	}

	@Override
	public DBWrittenStats onBatch(Iterator<HashMap<String, Object>> rows, long batchNumber, long currentBatchCount,
	        ArrayList<HashMap<String, Object>> batchedRows) throws RecordProcessingException, DataCorruptedException {
		try {
			if (sqlSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPSERT)) {

				long insertedCount = sqlBatchConnections.getUpsertStatement(TaskContext.getPartitionId()).executeBatch().length;
				return new DBWrittenStats(insertedCount, 0);
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

			if (sqlBatchConnections.getUpdateStatement(TaskContext.getPartitionId()) != null) {
				sqlBatchConnections.getUpsertStatement(TaskContext.getPartitionId()).clearBatch();
			}
		} catch (SQLException exception) {
			throw new RecordProcessingException(exception.getMessage(), exception);
		}
	}

	@Override
	public void onCallEnd(Iterator<HashMap<String, Object>> rows, BatchTrackingCountConfigBean trackingCounts) throws Exception {
		try {
			if (sqlBatchConnections.getSelectStatement(TaskContext.getPartitionId()) != null
			        && !sqlBatchConnections.getSelectStatement(TaskContext.getPartitionId()).isClosed()) {
				sqlBatchConnections.getSelectStatement(TaskContext.getPartitionId()).close();
			}

			if (sqlBatchConnections.getInsertStatement(TaskContext.getPartitionId()) != null
			        && !sqlBatchConnections.getInsertStatement(TaskContext.getPartitionId()).isClosed()) {
				sqlBatchConnections.getInsertStatement(TaskContext.getPartitionId()).close();
			}

			if (sqlBatchConnections.getUpdateStatement(TaskContext.getPartitionId()) != null
			        && !sqlBatchConnections.getUpdateStatement(TaskContext.getPartitionId()).isClosed()) {
				sqlBatchConnections.getUpdateStatement(TaskContext.getPartitionId()).close();
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
