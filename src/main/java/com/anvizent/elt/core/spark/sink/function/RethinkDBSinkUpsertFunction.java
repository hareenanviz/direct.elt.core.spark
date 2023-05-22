package com.anvizent.elt.core.spark.sink.function;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.spark.TaskContext;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.exception.DataCorruptedException;
import com.anvizent.elt.core.lib.exception.DateParseException;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidConfigValueException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.lib.exception.InvalidSituationException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.UnsupportedCoerceException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.lib.stats.beans.DBWrittenRow;
import com.anvizent.elt.core.lib.stats.beans.WrittenRow;
import com.anvizent.elt.core.listener.common.connection.ApplicationConnectionBean;
import com.anvizent.elt.core.listener.common.connection.RethinkDBConnectionByTaskId;
import com.anvizent.elt.core.spark.constant.Constants.NOSQL;
import com.anvizent.elt.core.spark.sink.config.bean.RethinkDBSinkConfigBean;
import com.anvizent.elt.core.spark.sink.service.NoSQLConstantsService;
import com.anvizent.elt.core.spark.sink.service.RethinkDBOnConflictReplace;
import com.anvizent.elt.core.spark.sink.service.RethinkDBSinkService;
import com.anvizent.elt.core.spark.sink.util.bean.RethinkDBSinkGetResult;
import com.anvizent.elt.core.spark.util.RowUtil;
import com.rethinkdb.gen.ast.Insert;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.net.Connection;

/**
 * @author Hareen Bejjanki
 *
 */
public class RethinkDBSinkUpsertFunction extends RethinkDBSinkFunction {

	private static final long serialVersionUID = 1L;

	public RethinkDBSinkUpsertFunction(RethinkDBSinkConfigBean rethinkDBSinkConfigBean, LinkedHashMap<String, AnvizentDataType> structure,
	        LinkedHashMap<String, AnvizentDataType> newStructure, ArrayList<AnvizentAccumulator> anvizentAccumulators,
	        AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails) throws InvalidRelationException {
		super(rethinkDBSinkConfigBean, null, structure, newStructure, anvizentAccumulators, errorHandlerSinkFunction, jobDetails);
	}

	@Override
	public WrittenRow process(HashMap<String, Object> row, RethinkDBSinkGetResult rethinkDBSinkGetResult)
	        throws RecordProcessingException, ValidationViolationException, DataCorruptedException {
		try {
			if (rethinkDBSinkGetResult.getResult() == null) {
				return insert(row, rethinkDBSinkGetResult.getrId());
			} else {
				return update(row, rethinkDBSinkGetResult);
			}
		} catch (DataCorruptedException | UnsupportedCoerceException | InvalidSituationException exception) {
			throw new DataCorruptedException(exception);
		} catch (ValidationViolationException | DateParseException | InvalidConfigValueException | ImproperValidationException exception) {
			throw new ValidationViolationException(exception.getMessage(), exception);
		} catch (Exception exception) {
			throw new RecordProcessingException(exception.getMessage(), exception);
		}
	}

	@SuppressWarnings("rawtypes")
	private DBWrittenRow insert(HashMap<String, Object> row, Object rId)
	        throws TimeoutException, ImproperValidationException, SQLException, DataCorruptedException, ValidationViolationException,
	        UnsupportedCoerceException, InvalidSituationException, DateParseException, InvalidConfigValueException {
		RethinkDBSinkConfigBean rethinkDBSinkConfigBean = (RethinkDBSinkConfigBean) configBean;

		HashMap<String, Object> differKeysAndFieldsRow = RowUtil.changeFieldsToDifferColumns(row, rethinkDBSinkConfigBean.getKeyFields(),
		        rethinkDBSinkConfigBean.getKeyColumns(), rethinkDBSinkConfigBean.getFieldsDifferToColumns(),
		        rethinkDBSinkConfigBean.getColumnsDifferToFields());

		HashMap<String, Object> newRow = RowUtil.addElements(differKeysAndFieldsRow,
		        NoSQLConstantsService.getConstants(rethinkDBSinkConfigBean.getExternalDataPrefix(), rethinkDBSinkConfigBean.getEmptyRow(),
		                rethinkDBSinkConfigBean.getEmptyArguments(), rethinkDBSinkConfigBean.getTimeZoneOffset(),
		                rethinkDBSinkConfigBean.getInsertConstantsConfigBean(), insertExpressionEvaluators),
		        newStructure);

		RethinkDBSinkService.convertToRethinkDBType(newRow, ((RethinkDBSinkConfigBean) configBean).getTimeZoneOffset());
		RethinkDBSinkService.ifRIdExistsThenPut(newRow, rId);

		MapObject rRow = getRRecord(newRow);

		Insert insert = getTable().insert(rRow);
		if (rethinkDBSinkConfigBean.isSoftDurability()) {
			insert = insert.optArg(NOSQL.RethinkDB.DURABILITY, NOSQL.RethinkDB.SOFT);
		}

		Map result = insert.optArg(NOSQL.RethinkDB.CONFLICT, new RethinkDBOnConflictReplace(rethinkDBSinkConfigBean)).run((Connection) ApplicationConnectionBean
		        .getInstance().get(new RethinkDBConnectionByTaskId(rethinkDBSinkConfigBean.getConnection(), null, TaskContext.getPartitionId()), true)[0]);

		if (result == null || ((Long) result.get(NOSQL.RethinkDB.Response.INSERTED) != 1 && (Long) result.get(NOSQL.RethinkDB.Response.UNCHANGED) != 1
		        && (Long) result.get(NOSQL.RethinkDB.Response.REPLACED) != 1)) {
			throw new DataCorruptedException("Unable to insert record into RethinkDB, details: " + RethinkDBSinkService.getErrorMessage(result));
		} else if ((Long) result.get(NOSQL.RethinkDB.Response.INSERTED) == 1) {
			return new DBWrittenRow(newStructure, row, newRow, false);
		} else if ((Long) result.get(NOSQL.RethinkDB.Response.REPLACED) == 1) {
			return new DBWrittenRow(newStructure, row, newRow, true);
		} else if ((Long) result.get(NOSQL.RethinkDB.Response.UNCHANGED) == 1) {
			return new DBWrittenRow(newStructure, row, null, false);
		} else {
			throw new DataCorruptedException("Unable to insert record into RethinkDB, details: " + RethinkDBSinkService.getErrorMessage(result));
		}
	}

	private WrittenRow update(HashMap<String, Object> row, RethinkDBSinkGetResult rethinkDBSinkGetResult)
	        throws UnsupportedCoerceException, ImproperValidationException, TimeoutException, SQLException, DataCorruptedException,
	        ValidationViolationException, InvalidSituationException, DateParseException, InvalidConfigValueException, UnsupportedException {
		if (rethinkDBSinkGetResult.getrId() != null || rethinkDBSinkGetResult.isDoUpdate()) {
			return update(row, rethinkDBSinkGetResult.getrId());
		} else {
			return new DBWrittenRow(newStructure, row, null, false);
		}
	}

	@SuppressWarnings("rawtypes")
	private WrittenRow update(HashMap<String, Object> row, Object rId)
	        throws ImproperValidationException, TimeoutException, SQLException, DataCorruptedException, ValidationViolationException,
	        UnsupportedCoerceException, InvalidSituationException, DateParseException, InvalidConfigValueException {
		RethinkDBSinkConfigBean rethinkDBSinkConfigBean = ((RethinkDBSinkConfigBean) configBean);

		HashMap<String, Object> differKeysAndFieldsRow = RowUtil.changeFieldsToDifferColumns(row, rethinkDBSinkConfigBean.getKeyFields(),
		        rethinkDBSinkConfigBean.getKeyColumns(), rethinkDBSinkConfigBean.getFieldsDifferToColumns(),
		        rethinkDBSinkConfigBean.getColumnsDifferToFields());

		HashMap<String, Object> newRow = RowUtil.addConstantElements(differKeysAndFieldsRow,
		        NoSQLConstantsService.getConstants(rethinkDBSinkConfigBean.getExternalDataPrefix(), rethinkDBSinkConfigBean.getEmptyRow(),
		                rethinkDBSinkConfigBean.getEmptyArguments(), rethinkDBSinkConfigBean.getTimeZoneOffset(),
		                rethinkDBSinkConfigBean.getUpdateConstantsConfigBean(), updateExpressionEvaluators));

		RethinkDBSinkService.convertToRethinkDBType(newRow, rethinkDBSinkConfigBean.getTimeZoneOffset());

		MapObject rRow = getRRecord(newRow);
		Map result;

		result = getTable().get(rId).update(rRow).run((Connection) ApplicationConnectionBean.getInstance()
		        .get(new RethinkDBConnectionByTaskId(rethinkDBSinkConfigBean.getConnection(), null, TaskContext.getPartitionId()), true)[0]);

		if (result == null || ((Long) result.get(NOSQL.RethinkDB.Response.REPLACED) != 1 && (Long) result.get(NOSQL.RethinkDB.Response.UNCHANGED) != 1)) {
			throw new DataCorruptedException("Unable to insert/update record into RethinkDB, details: " + RethinkDBSinkService.getErrorMessage(result));
		} else if ((Long) result.get(NOSQL.RethinkDB.Response.REPLACED) == 1) {
			return new DBWrittenRow(newStructure, row, newRow, true);
		} else if ((Long) result.get(NOSQL.RethinkDB.Response.UNCHANGED) == 1) {
			return new DBWrittenRow(newStructure, row, null, false);
		} else {
			throw new DataCorruptedException("Unable to insert/update record into RethinkDB, details: " + RethinkDBSinkService.getErrorMessage(result));
		}
	}
}
