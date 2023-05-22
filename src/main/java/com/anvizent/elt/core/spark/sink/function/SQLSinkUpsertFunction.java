package com.anvizent.elt.core.spark.sink.function;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeoutException;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidConfigValueException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.lib.stats.beans.DBWrittenRow;
import com.anvizent.elt.core.lib.stats.beans.WrittenRow;
import com.anvizent.elt.core.spark.common.util.SQLUtil;
import com.anvizent.elt.core.spark.constant.Constants.ExceptionMessage;
import com.anvizent.elt.core.spark.sink.config.bean.SQLSinkConfigBean;
import com.anvizent.elt.core.spark.sink.service.SQLUpdateService;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class SQLSinkUpsertFunction extends SQLSinkFunction {

	private static final long serialVersionUID = 1L;

	private PreparedStatement insertPreparedStatement;
	private PreparedStatement updatePreparedStatement;

	public SQLSinkUpsertFunction(SQLSinkConfigBean sqlSinkConfigBean, LinkedHashMap<String, AnvizentDataType> structure,
	        ArrayList<AnvizentAccumulator> anvizentAccumulators, AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails)
	        throws InvalidRelationException {
		super(sqlSinkConfigBean, null, structure, structure, anvizentAccumulators, errorHandlerSinkFunction, jobDetails);
	}

	@Override
	public WrittenRow process(HashMap<String, Object> row, ResultSet resultSet) throws RecordProcessingException, ValidationViolationException {
		try {
			if (resultSet == null || !resultSet.next()) {
				return insert(row);
			} else {
				return update(row, resultSet);
			}
		} catch (SQLException exception) {
			throw new RecordProcessingException(exception.getMessage(), exception);
		} catch (ImproperValidationException | UnimplementedException | TimeoutException | InvalidConfigValueException exception) {
			throw new ValidationViolationException(exception.getMessage(), exception);
		}
	}

	private WrittenRow insert(HashMap<String, Object> row) throws SQLException, ImproperValidationException, UnimplementedException, TimeoutException {
		createAndSetInsertPreparedStatement(row);

		int insert = insertPreparedStatement.executeUpdate();
		if (insert <= 0) {
			throw new SQLException(ExceptionMessage.FAILED_TO_INSERT);
		}

		return new DBWrittenRow(newStructure, row, row, false);
	}

	private void createAndSetInsertPreparedStatement(HashMap<String, Object> row)
	        throws SQLException, ImproperValidationException, UnimplementedException, TimeoutException {
		insertPreparedStatement = checkStatementAndReconnect(insertPreparedStatement, sqlSinkConfigBean.getInsertQuery());
		SQLUtil.setPreparedStatement(insertPreparedStatement, row, sqlSinkConfigBean.getRowKeysAndFields());
	}

	private WrittenRow update(HashMap<String, Object> row, ResultSet resultSet)
	        throws SQLException, ImproperValidationException, RecordProcessingException, UnimplementedException, TimeoutException, InvalidConfigValueException {
		boolean doUpdate = SQLUpdateService.checkForUpdate(resultSet, row, sqlSinkConfigBean.isAlwaysUpdate(), sqlSinkConfigBean.getChecksumField(),
		        sqlSinkConfigBean.getMetaDataFields(), sqlSinkConfigBean.getRowFields(), sqlSinkConfigBean.getFieldsDifferToColumns(),
		        sqlSinkConfigBean.getColumnsDifferToFields());
		if (doUpdate) {
			createAndSetUpdatePreparedStatement(row, sqlSinkConfigBean.getRowFields());
			SQLUpdateService.update(updatePreparedStatement);

			return new DBWrittenRow(newStructure, row, row, true);
		} else {
			return new DBWrittenRow(newStructure, row, null, false);
		}
	}

	public void createAndSetUpdatePreparedStatement(HashMap<String, Object> row, ArrayList<String> rowKeys)
	        throws SQLException, ImproperValidationException, UnimplementedException, TimeoutException {
		updatePreparedStatement = checkStatementAndReconnect(updatePreparedStatement, sqlSinkConfigBean.getUpdateQuery());
		SQLUtil.setPreparedStatement(updatePreparedStatement, row, rowKeys);
		SQLUtil.setPreparedStatement(updatePreparedStatement, row, sqlSinkConfigBean.getKeyFields(), rowKeys.size() + 1);
	}
}
