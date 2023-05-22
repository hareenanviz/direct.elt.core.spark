package com.anvizent.elt.core.spark.sink.function;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeoutException;

import org.apache.spark.TaskContext;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;
import com.anvizent.elt.core.lib.constant.StoreType;
import com.anvizent.elt.core.lib.exception.DataCorruptedException;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.lib.stats.beans.WrittenRow;
import com.anvizent.elt.core.listener.common.connection.ApplicationConnectionBean;
import com.anvizent.elt.core.listener.common.connection.RDBMSConnectionByTaskId;
import com.anvizent.elt.core.spark.common.util.SQLUtil;
import com.anvizent.elt.core.spark.sink.config.bean.SQLSinkConfigBean;

/**
 * @author Hareen Bejjanki
 *
 */
public abstract class SQLSinkFunction extends AnvizentVoidFunction {

	private static final long serialVersionUID = 1L;

	protected SQLSinkConfigBean sqlSinkConfigBean;
	private PreparedStatement selectPreparedStatement;

	public SQLSinkFunction(SQLSinkConfigBean sqlSinkConfigBean, MappingConfigBean mappingConfigBean, LinkedHashMap<String, AnvizentDataType> structure,
	        LinkedHashMap<String, AnvizentDataType> newStructure, ArrayList<AnvizentAccumulator> anvizentAccumulators,
	        AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails) throws InvalidRelationException {
		super(sqlSinkConfigBean, null, structure, newStructure, null, anvizentAccumulators, errorHandlerSinkFunction, jobDetails);
		this.sqlSinkConfigBean = sqlSinkConfigBean;
	}

	@Override
	public WrittenRow process(HashMap<String, Object> row) throws ValidationViolationException, RecordProcessingException, DataCorruptedException {
		try {
			WrittenRow writtenRow = null;

			if (this instanceof SQLSinkInsertFunction && (sqlSinkConfigBean.getKeyFields() == null || sqlSinkConfigBean.getKeyFields().isEmpty())) {
				writtenRow = process(row, null);
			} else {
				ResultSet resultSet = getResultSet(row);

				writtenRow = process(row, resultSet);
				SQLUtil.closeResultSetObject(resultSet);
			}

			return writtenRow;
		} catch (SQLException exception) {
			StoreType storeType = StoreType.getInstance(sqlSinkConfigBean.getRdbmsConnection().getDriver());
			if (storeType != null && StoreType.DATA_CORRUPTED_ERROR_CODES.get(storeType) != null
			        && StoreType.DATA_CORRUPTED_ERROR_CODES.get(storeType).contains(exception.getErrorCode())) {
				throw new DataCorruptedException(exception.getMessage(), exception);
			} else {
				throw new RecordProcessingException(exception.getMessage(), exception);
			}
		} catch (TimeoutException exception) {
			throw new RecordProcessingException(exception.getMessage(), exception);
		}
	}

	protected PreparedStatement checkStatementAndReconnect(PreparedStatement preparedStatement, String query)
	        throws ImproperValidationException, SQLException, UnimplementedException, TimeoutException {
		if (preparedStatement == null || preparedStatement.isClosed()) {
			preparedStatement = ((Connection) ApplicationConnectionBean.getInstance()
			        .get(new RDBMSConnectionByTaskId(sqlSinkConfigBean.getRdbmsConnection(), null, TaskContext.getPartitionId()), true)[0])
			                .prepareStatement(query);
		}

		return preparedStatement;
	}

	private ResultSet getResultSet(HashMap<String, Object> row) throws SQLException, ImproperValidationException, UnimplementedException, TimeoutException {
		selectPreparedStatement = checkStatementAndReconnect(selectPreparedStatement, sqlSinkConfigBean.getSelectQuery());
		setSelectPreparedStatement(row);
		return selectPreparedStatement.executeQuery();
	}

	private void setSelectPreparedStatement(HashMap<String, Object> row) throws SQLException {
		for (int i = 1; i <= sqlSinkConfigBean.getKeyFields().size(); i++) {
			selectPreparedStatement.setObject(i, row.get(sqlSinkConfigBean.getKeyFields().get(i - 1)));
		}
	}

	public abstract WrittenRow process(HashMap<String, Object> row, ResultSet resultSet)
	        throws RecordProcessingException, ValidationViolationException, DataCorruptedException;
}
