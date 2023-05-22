package com.anvizent.elt.core.spark.sink.function;

import java.sql.PreparedStatement;
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
import com.anvizent.elt.core.lib.constant.StoreType;
import com.anvizent.elt.core.lib.exception.DataCorruptedException;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
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
import com.anvizent.elt.core.spark.sink.config.bean.SQLSinkConfigBean;
import com.anvizent.elt.core.spark.sink.config.bean.SQLUpsertWithDBCheckConnectionAndStatments;
import com.anvizent.elt.core.spark.sink.function.bean.SQLSinkDBQueryFunctionBean;
import com.anvizent.elt.core.spark.sink.function.util.SQLSinkWithDBCheckFunctionUtil;

/**
 * @author Hareen Bejjanki
 *
 */
public class SQLSinkWithDBCheckBatchFunction extends BaseAnvizentBatchFunction<HashMap<String, Object>, HashMap<String, Object>> {

	private static final long serialVersionUID = 1L;

	private SQLSinkDBQueryFunctionBean functionBean;
	private RDBMSConnection rdbmsConnection;
	private SQLUpsertWithDBCheckConnectionAndStatments sqlBatchConnections;

	public SQLSinkWithDBCheckBatchFunction(SQLSinkConfigBean sqlSinkConfigBean, SQLSinkDBQueryFunctionBean functionBean,
	        LinkedHashMap<String, AnvizentDataType> structure, ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction errorHandlerSinkFunction,
	        JobDetails jobDetails) throws InvalidRelationException, InvalidArgumentsException {
		super(sqlSinkConfigBean, null, structure, structure, null, sqlSinkConfigBean.getMaxRetryCount(), sqlSinkConfigBean.getRetryDelay(),
		        sqlSinkConfigBean.getInitRetryConfigBean().getMaxRetryCount(), sqlSinkConfigBean.getInitRetryConfigBean().getRetryDelay(),
		        sqlSinkConfigBean.getDestroyRetryConfigBean().getMaxRetryCount(), sqlSinkConfigBean.getDestroyRetryConfigBean().getRetryDelay(),
		        sqlSinkConfigBean.getBatchSize(), accumulators, errorHandlerSinkFunction, jobDetails);

		this.functionBean = functionBean;
		this.rdbmsConnection = sqlSinkConfigBean.getRdbmsConnection();
		this.sqlBatchConnections = new SQLUpsertWithDBCheckConnectionAndStatments();
	}

	@Override
	public void onCallStart(Iterator<HashMap<String, Object>> rows, BatchTrackingCountConfigBean trackingCounts) throws ImproperValidationException, Exception {
		SQLSinkWithDBCheckFunctionUtil.init(sqlBatchConnections, rdbmsConnection, functionBean);
	}

	@Override
	public boolean process(HashMap<String, Object> row, ArrayList<HashMap<String, Object>> batchedRows)
	        throws RecordProcessingException, ValidationViolationException {
		try {
			return upsert(row);
		} catch (SQLException exception) {
			throw new RecordProcessingException(exception.getMessage(), exception);
		}
	}

	private boolean upsert(HashMap<String, Object> row) throws SQLException {
		String deleteField = functionBean.getDeleteIndicatorField();

		if (StringUtils.isNotBlank(deleteField) && row.get(deleteField) != null && (Boolean) row.get(deleteField)) {
			PreparedStatement statement = sqlBatchConnections.getDeleteStatement(TaskContext.getPartitionId());
			SQLUtil.setPreparedStatement(statement, row, functionBean.getDeleteQuery().getFieldsToSet());
			statement.addBatch();
		} else {
			PreparedStatement statement = sqlBatchConnections.getUpsertStatement(TaskContext.getPartitionId());
			SQLUtil.setPreparedStatement(statement, row, functionBean.getQueryInfo().getMultiValuedFieldIndexesToSet());
			statement.addBatch();
		}

		return true;
	}

	@Override
	public void beforeBatch(Iterator<HashMap<String, Object>> rows) {
	}

	@Override
	public DBWrittenStats onBatch(Iterator<HashMap<String, Object>> rows, long batchNumber, long currentBatchCount,
	        ArrayList<HashMap<String, Object>> batchedRows) throws RecordProcessingException, DataCorruptedException {
		try {
			if (StringUtils.isNotBlank(functionBean.getDeleteIndicatorField())) {
				long deletedCount = sqlBatchConnections.getDeleteStatement(TaskContext.getPartitionId()) != null
				        ? sqlBatchConnections.getDeleteStatement(TaskContext.getPartitionId()).executeBatch().length
				        : 0;
				System.out.println("Delete count: " + deletedCount);
			}

			long insertedCount = sqlBatchConnections.getUpsertStatement(TaskContext.getPartitionId()).executeBatch().length;
			return new DBWrittenStats(insertedCount, 0);
		} catch (SQLException sqlException) {
			if (functionBean.getStoreType() != null && StoreType.DATA_CORRUPTED_ERROR_CODES.get(functionBean.getStoreType()) != null
			        && StoreType.DATA_CORRUPTED_ERROR_CODES.get(functionBean.getStoreType()).contains(sqlException.getErrorCode())) {
				throw new DataCorruptedException(sqlException.getMessage(), sqlException);
			} else {
				throw new RecordProcessingException(sqlException.getMessage(), sqlException);
			}
		}
	}

	@Override
	public void afterBatch(Iterator<HashMap<String, Object>> rows, ArrayList<HashMap<String, Object>> batchedRows) throws Exception {
		try {
			if (sqlBatchConnections.getUpsertStatement(TaskContext.getPartitionId()) != null) {
				sqlBatchConnections.getUpsertStatement(TaskContext.getPartitionId()).clearBatch();
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
			if (sqlBatchConnections.getUpsertStatement(TaskContext.getPartitionId()) != null
			        && !sqlBatchConnections.getUpsertStatement(TaskContext.getPartitionId()).isClosed()) {
				sqlBatchConnections.getUpsertStatement(TaskContext.getPartitionId()).close();
			}

			if (StringUtils.isNotBlank(functionBean.getDeleteIndicatorField()) && sqlBatchConnections.getDeleteStatement(TaskContext.getPartitionId()) != null
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
