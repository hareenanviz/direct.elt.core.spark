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
import com.anvizent.elt.core.lib.exception.DataCorruptedException;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.lib.stats.beans.DBWrittenRow;
import com.anvizent.elt.core.lib.stats.beans.WrittenRow;
import com.anvizent.elt.core.spark.common.util.SQLUtil;
import com.anvizent.elt.core.spark.exception.RecordAlreadyExistsException;
import com.anvizent.elt.core.spark.sink.config.bean.SQLSinkConfigBean;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class SQLSinkInsertFunction extends SQLSinkFunction {

	private static final long serialVersionUID = 1L;

	private PreparedStatement insertPreparedStatement;

	public SQLSinkInsertFunction(SQLSinkConfigBean sqlSinkConfigBean, LinkedHashMap<String, AnvizentDataType> structure,
			ArrayList<AnvizentAccumulator> anvizentAccumulators, AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails)
			throws InvalidRelationException {
		super(sqlSinkConfigBean, null, structure, structure, anvizentAccumulators, errorHandlerSinkFunction, jobDetails);
	}

	@Override
	public WrittenRow process(HashMap<String, Object> row, ResultSet resultSet)
			throws RecordProcessingException, ValidationViolationException, DataCorruptedException {
		try {
			if (resultSet == null || !resultSet.next()) {
				insert(row);
				return new DBWrittenRow(newStructure, row, row, false);
			} else {
				throw new RecordAlreadyExistsException("Record already exists.");
			}
		} catch (RecordAlreadyExistsException exception) {
			throw new RecordProcessingException(exception.getMessage(), new DataCorruptedException(exception));
		} catch (SQLException exception) {
			throw new RecordProcessingException(exception.getMessage(), exception);
		} catch (UnimplementedException | ImproperValidationException | TimeoutException exception) {
			throw new ValidationViolationException(exception.getMessage(), exception);
		}
	}

	private void insert(HashMap<String, Object> row) throws SQLException, ImproperValidationException, UnimplementedException, TimeoutException {
		insertPreparedStatement = checkStatementAndReconnect(insertPreparedStatement, sqlSinkConfigBean.getInsertQuery());
		SQLUtil.setPreparedStatement(insertPreparedStatement, row, sqlSinkConfigBean.getRowKeysAndFields());
		insertPreparedStatement.executeUpdate();
	}
}
