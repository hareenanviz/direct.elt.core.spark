package com.anvizent.elt.core.spark.operation.function;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeoutException;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.exception.DataCorruptedException;
import com.anvizent.elt.core.lib.exception.DateParseException;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.InvalidConfigValueException;
import com.anvizent.elt.core.lib.exception.InvalidSituationException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedCoerceException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.lib.function.BaseAnvizentRetrievalFunction;
import com.anvizent.elt.core.lib.row.formatter.AnvizentErrorSetter;
import com.anvizent.elt.core.lib.row.formatter.BaseAnvizentErrorSetter;
import com.anvizent.elt.core.lib.stats.beans.BaseDBRetrievalRow;
import com.anvizent.elt.core.lib.stats.beans.DBRetrievalRow;
import com.anvizent.elt.core.spark.constant.Constants.General;
import com.anvizent.elt.core.spark.exception.InvalidLookUpException;
import com.anvizent.elt.core.spark.operation.config.bean.RethinkLookUpConfigBean;
import com.anvizent.elt.core.spark.operation.service.RethinkLookUpService;
import com.anvizent.elt.core.spark.operation.service.RethinkRetrievalFunctionService;
import com.anvizent.elt.core.spark.util.RowUtil;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.Table;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
@SuppressWarnings("unchecked")
public class RethinkLookUpFunction extends BaseAnvizentRetrievalFunction<HashMap<String, Object>, HashMap<String, Object>, HashMap<String, Object>> {

	private static final long serialVersionUID = 1L;

	private RethinkLookUpConfigBean rethinkLookUpConfigBean;
	private RethinkLookUpService lookUpService;
	private Table table;

	public RethinkLookUpFunction(ConfigBean configBean, LinkedHashMap<String, AnvizentDataType> structure, LinkedHashMap<String, AnvizentDataType> newStructure,
			ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails)
			throws InvalidArgumentsException {
		super(configBean, null, structure, newStructure, null, accumulators, errorHandlerSinkFunction, jobDetails);
		this.rethinkLookUpConfigBean = (RethinkLookUpConfigBean) configBean;
	}

	@Override
	public BaseDBRetrievalRow<HashMap<String, Object>, HashMap<String, Object>> process(HashMap<String, Object> row)
			throws ValidationViolationException, RecordProcessingException, DataCorruptedException {
		try {
			if (lookUpService == null) {
				lookUpService = new RethinkLookUpService(rethinkLookUpConfigBean);
			}

			lookUpService.createConnection();

			return lookUp(row);

		} catch (TimeoutException | SQLException exception) {
			throw new RecordProcessingException(exception.getMessage(), exception);
		} catch (InvalidConfigValueException exception) {
			throw new ValidationViolationException(exception.getMessage(), exception);
		} catch (InvalidSituationException | UnsupportedCoerceException | InvalidLookUpException exception) {
			throw new DataCorruptedException(exception);
		}
	}

	private BaseDBRetrievalRow<HashMap<String, Object>, HashMap<String, Object>> lookUp(HashMap<String, Object> row)
			throws InvalidLookUpException, UnimplementedException, UnsupportedCoerceException, InvalidSituationException, DateParseException,
			ImproperValidationException, InvalidConfigValueException, SQLException, TimeoutException {
		boolean inserted = false;

		HashMap<String, Object> newObject = lookUpService.selectFields(row, getTable());
		HashMap<String, Object> newRow = (LinkedHashMap<String, Object>) newObject.get(General.LOOKUP_RESULTING_ROW);
		if (newRow == null || newRow.isEmpty()) {
			newRow = lookUpService.insertFields(row, getTable());
			inserted = true;
		}

		HashMap<String, Object> replacedNewRow = RethinkRetrievalFunctionService.replaceWithAliasNames(newRow, rethinkLookUpConfigBean.getSelectFields(),
				rethinkLookUpConfigBean.getSelectFieldAliases());

		return new DBRetrievalRow<HashMap<String, Object>, HashMap<String, Object>>(row, RowUtil.addElements(row, replacedNewRow, newStructure), inserted,
				(Boolean) newObject.get(General.LOOKUP_CACHED), 1, (Integer) newObject.get(General.LOOKEDUP_ROWS_COUNT));
	}

	private Table getTable() {
		if (table == null) {
			table = RethinkDB.r.table(rethinkLookUpConfigBean.getTableName());
		}

		return table;
	}

	@Override
	protected BaseAnvizentErrorSetter<HashMap<String, Object>, HashMap<String, Object>> getBaseAnvizentErrorRowSetter() throws InvalidArgumentsException {
		return AnvizentErrorSetter.getDefaultInstance();
	}
}
