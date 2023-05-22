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
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.lib.stats.beans.DBWrittenRow;
import com.anvizent.elt.core.lib.stats.beans.WrittenRow;
import com.anvizent.elt.core.listener.common.connection.ApplicationConnectionBean;
import com.anvizent.elt.core.listener.common.connection.RethinkDBConnectionByTaskId;
import com.anvizent.elt.core.spark.constant.Constants.NOSQL;
import com.anvizent.elt.core.spark.exception.RecordAlreadyExistsException;
import com.anvizent.elt.core.spark.sink.config.bean.RethinkDBSinkConfigBean;
import com.anvizent.elt.core.spark.sink.service.NoSQLConstantsService;
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
public class RethinkDBSinkInsertFunction extends RethinkDBSinkFunction {

	private static final long serialVersionUID = 1L;

	public RethinkDBSinkInsertFunction(RethinkDBSinkConfigBean rethinkDBSinkConfigBean, LinkedHashMap<String, AnvizentDataType> structure,
	        LinkedHashMap<String, AnvizentDataType> newStructure, ArrayList<AnvizentAccumulator> anvizentAccumulators,
	        AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails) throws InvalidRelationException {
		super(rethinkDBSinkConfigBean, null, structure, newStructure, anvizentAccumulators, errorHandlerSinkFunction, jobDetails);
	}

	@Override
	public WrittenRow process(HashMap<String, Object> row, RethinkDBSinkGetResult rethinkDBSinkGetResult)
	        throws RecordProcessingException, ValidationViolationException, DataCorruptedException {
		try {
			if (rethinkDBSinkGetResult.getResult() == null) {
				insert(row, rethinkDBSinkGetResult.getrId());
				return new DBWrittenRow(newStructure, row, row, false);
			} else {
				throw new RecordAlreadyExistsException("Record already exists.");
			}
		} catch (DataCorruptedException | UnsupportedCoerceException | InvalidSituationException | RecordAlreadyExistsException exception) {
			throw new DataCorruptedException(exception);
		} catch (ValidationViolationException | DateParseException | InvalidConfigValueException | ImproperValidationException exception) {
			throw new ValidationViolationException(exception.getMessage(), exception);
		} catch (Exception exception) {
			throw new RecordProcessingException(exception.getMessage(), exception);
		}
	}

	@SuppressWarnings("rawtypes")
	private void insert(HashMap<String, Object> row, Object rId) throws TimeoutException, ImproperValidationException, SQLException, DataCorruptedException,
	        ValidationViolationException, UnsupportedCoerceException, InvalidSituationException, DateParseException, InvalidConfigValueException {
		RethinkDBSinkConfigBean rethinkDBSinkConfigBean = (RethinkDBSinkConfigBean) configBean;

		HashMap<String, Object> differKeysAndFieldsRow = RowUtil.changeFieldsToDifferColumns(row, rethinkDBSinkConfigBean.getKeyFields(),
		        rethinkDBSinkConfigBean.getKeyColumns(), rethinkDBSinkConfigBean.getFieldsDifferToColumns(),
		        rethinkDBSinkConfigBean.getColumnsDifferToFields());

		HashMap<String, Object> newRow = RowUtil.addElements(differKeysAndFieldsRow,
		        NoSQLConstantsService.getConstants(rethinkDBSinkConfigBean.getExternalDataPrefix(), rethinkDBSinkConfigBean.getEmptyRow(),
		                rethinkDBSinkConfigBean.getEmptyArguments(), rethinkDBSinkConfigBean.getTimeZoneOffset(),
		                rethinkDBSinkConfigBean.getConstantsConfigBean(), expressionEvaluators),
		        newStructure);

		RethinkDBSinkService.convertToRethinkDBType(newRow, rethinkDBSinkConfigBean.getTimeZoneOffset());
		RethinkDBSinkService.ifRIdExistsThenPut(newRow, rId);

		MapObject rRow = getRRecord(newRow);

		Insert insert = getTable().insert(rRow);
		if (rethinkDBSinkConfigBean.isSoftDurability()) {
			insert = insert.optArg(NOSQL.RethinkDB.DURABILITY, NOSQL.RethinkDB.SOFT);
		}

		Map result = insert.run((Connection) ApplicationConnectionBean.getInstance()
		        .get(new RethinkDBConnectionByTaskId(rethinkDBSinkConfigBean.getConnection(), null, TaskContext.getPartitionId()), true)[0]);
		if (result == null || ((Long) result.get(NOSQL.RethinkDB.Response.INSERTED) != 1 && (Long) result.get(NOSQL.RethinkDB.Response.UNCHANGED) != 1)) {
			throw new DataCorruptedException("Unable to insert record into RethinkDB, details: " + RethinkDBSinkService.getErrorMessage(result));
		}
	}
}
