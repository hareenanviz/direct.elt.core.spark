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
import com.anvizent.elt.core.spark.exception.RecordAlreadyExistsException;
import com.anvizent.elt.core.spark.sink.config.bean.SQLInsertBatchConnections;
import com.anvizent.elt.core.spark.sink.config.bean.SQLSinkConfigBean;

/**
 * @author Hareen Bejjanki
 *
 */
public class SQLSinkInsertBatchFunction extends BaseAnvizentBatchFunction<HashMap<String, Object>, HashMap<String, Object>> {

	private static final long serialVersionUID = 1L;

	private SQLSinkConfigBean sqlSinkConfigBean;
	private RDBMSConnection rdbmsConnection;
	private SQLInsertBatchConnections sqlBatchConnections;
	private ArrayList<ArrayList<Object>> keysAdded = new ArrayList<ArrayList<Object>>();

	public SQLSinkInsertBatchFunction(SQLSinkConfigBean sqlSinkConfigBean, LinkedHashMap<String, AnvizentDataType> structure,
	        ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails)
	        throws InvalidRelationException, InvalidArgumentsException {
		super(sqlSinkConfigBean, null, structure, structure, null, sqlSinkConfigBean.getMaxRetryCount(), sqlSinkConfigBean.getRetryDelay(),
		        sqlSinkConfigBean.getInitRetryConfigBean().getMaxRetryCount(), sqlSinkConfigBean.getInitRetryConfigBean().getRetryDelay(),
		        sqlSinkConfigBean.getDestroyRetryConfigBean().getMaxRetryCount(), sqlSinkConfigBean.getDestroyRetryConfigBean().getRetryDelay(),
		        sqlSinkConfigBean.getBatchSize(), accumulators, errorHandlerSinkFunction, jobDetails);

		this.sqlSinkConfigBean = sqlSinkConfigBean;
		this.rdbmsConnection = sqlSinkConfigBean.getRdbmsConnection();
		this.sqlBatchConnections = new SQLInsertBatchConnections();
	}

	@Override
	public void onCallStart(Iterator<HashMap<String, Object>> rows, BatchTrackingCountConfigBean trackingCounts) throws ImproperValidationException, Exception {
		try {
			if (sqlBatchConnections.getConnection(TaskContext.getPartitionId()) == null
			        || sqlBatchConnections.getConnection(TaskContext.getPartitionId()).isClosed()) {
				sqlBatchConnections.addConnection(TaskContext.getPartitionId(), getConnection());

				sqlBatchConnections.addSelectStatement(TaskContext.getPartitionId(), getPreparedStatement(sqlSinkConfigBean.getSelectQuery()));
				sqlBatchConnections.addInsertStatement(TaskContext.getPartitionId(), getPreparedStatement(sqlSinkConfigBean.getInsertQuery()));

				if (StringUtils.isNotBlank(sqlSinkConfigBean.getDeleteQuery())) {
					sqlBatchConnections.addDeleteStatement(TaskContext.getPartitionId(), getPreparedStatement(sqlSinkConfigBean.getDeleteQuery()));
				}

				sqlBatchConnections.getConnection(TaskContext.getPartitionId()).setAutoCommit(Boolean.FALSE);
			} else {
				if (sqlBatchConnections.getSelectStatement(TaskContext.getPartitionId()) == null
				        || sqlBatchConnections.getSelectStatement(TaskContext.getPartitionId()).isClosed()) {
					sqlBatchConnections.addSelectStatement(TaskContext.getPartitionId(), getPreparedStatement(sqlSinkConfigBean.getSelectQuery()));
				}

				if (sqlBatchConnections.getInsertStatement(TaskContext.getPartitionId()) == null
				        || sqlBatchConnections.getInsertStatement(TaskContext.getPartitionId()).isClosed()) {
					sqlBatchConnections.addInsertStatement(TaskContext.getPartitionId(), getPreparedStatement(sqlSinkConfigBean.getInsertQuery()));
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
		ArrayList<Object> keys = null;

		try {
			keys = getKeys(row);
			boolean recordExists = recordExists(row, keys);

			if (sqlSinkConfigBean.getDBInsertMode().equals(DBInsertMode.INSERT)) {
				return insert(row, recordExists);
			} else {
				throw new UnimplementedException();
			}
		} catch (SQLException exception) {
			if (keys != null && !keys.isEmpty()) {
				keysAdded.remove(keys);
			}

			throw new RecordProcessingException(exception.getMessage(), exception);
		} catch (RecordAlreadyExistsException exception) {
			if (keys != null && !keys.isEmpty()) {
				keysAdded.remove(keys);
			}

			throw new RecordProcessingException(exception.getMessage(), new DataCorruptedException(exception));
		} catch (UnimplementedException | InvalidConfigValueException exception) {
			if (keys != null && !keys.isEmpty()) {
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

	private void addInsertBatch(HashMap<String, Object> row) throws SQLException {
		SQLUtil.setPreparedStatement(sqlBatchConnections.getInsertStatement(TaskContext.getPartitionId()), row, sqlSinkConfigBean.getRowKeysAndFields());
		sqlBatchConnections.getInsertStatement(TaskContext.getPartitionId()).addBatch();
	}

	private ArrayList<Object> getKeys(HashMap<String, Object> row) throws SQLException, RecordProcessingException, InvalidConfigValueException {
		ArrayList<Object> keys = new ArrayList<Object>();

		for (int i = 0; i < sqlSinkConfigBean.getKeyFields().size(); i++) {
			if (rdbmsConnection.getDriver().equals(Constants.MYSQL_DRIVER) && !sqlSinkConfigBean.isKeyFieldsCaseSensitive()
			        && row.get(sqlSinkConfigBean.getKeyFields().get(i)).getClass().equals(String.class)) {
				keys.add(((String) row.get(sqlSinkConfigBean.getKeyFields().get(i))).toUpperCase());
			} else {
				keys.add(row.get(sqlSinkConfigBean.getKeyFields().get(i)));
			}
		}

		return keys;
	}

	private boolean recordExists(HashMap<String, Object> row, ArrayList<Object> keys)
	        throws SQLException, RecordProcessingException, InvalidConfigValueException {
		boolean recordExists;

		if (keysAdded.contains(keys)) {
			recordExists = true;
		} else {
			recordExists = recordExistsInDB(row, keys);
			keysAdded.add(keys);
		}

		return recordExists;
	}

	private boolean recordExistsInDB(HashMap<String, Object> row, ArrayList<Object> keys)
	        throws SQLException, RecordProcessingException, InvalidConfigValueException {
		setPreparedStatement(sqlBatchConnections.getSelectStatement(TaskContext.getPartitionId()), keys);
		ResultSet resultSet = sqlBatchConnections.getSelectStatement(TaskContext.getPartitionId()).executeQuery();
		boolean exists = resultSet != null && resultSet.next();

		SQLUtil.closeResultSetObject(resultSet);

		return exists;
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
			if (StringUtils.isNotBlank(sqlSinkConfigBean.getDeleteIndicatorField())) {
				long deletedCount = sqlBatchConnections.getDeleteStatement(TaskContext.getPartitionId()) != null
				        ? sqlBatchConnections.getDeleteStatement(TaskContext.getPartitionId()).executeBatch().length
				        : 0;
				System.out.println("Delete count: " + deletedCount);
			}
			long insertedCount = sqlBatchConnections.getInsertStatement(TaskContext.getPartitionId()) != null
			        ? sqlBatchConnections.getInsertStatement(TaskContext.getPartitionId()).executeBatch().length
			        : 0;

			return new DBWrittenStats(insertedCount, 0);
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
			if (sqlBatchConnections.getSelectStatement(TaskContext.getPartitionId()) != null
			        && !sqlBatchConnections.getSelectStatement(TaskContext.getPartitionId()).isClosed()) {
				sqlBatchConnections.getSelectStatement(TaskContext.getPartitionId()).close();
			}

			if (sqlBatchConnections.getInsertStatement(TaskContext.getPartitionId()) != null
			        && !sqlBatchConnections.getInsertStatement(TaskContext.getPartitionId()).isClosed()) {
				sqlBatchConnections.getInsertStatement(TaskContext.getPartitionId()).close();
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
