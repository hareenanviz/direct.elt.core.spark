package com.anvizent.elt.core.spark.operation.function;

import java.io.IOException;
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
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.InvalidConfigValueException;
import com.anvizent.elt.core.lib.exception.InvalidSituationException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
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
import com.anvizent.elt.core.spark.operation.config.bean.ArangoDBLookUpConfigBean;
import com.anvizent.elt.core.spark.operation.config.bean.ArangoDBRetrievalConfigBean;
import com.anvizent.elt.core.spark.operation.service.ArangoDBLookUpService;
import com.anvizent.elt.core.spark.util.RowUtil;
import com.anvizent.query.builder.exception.UnderConstructionException;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
@SuppressWarnings("unchecked")
public class ArangoDBLookUpFunction extends BaseAnvizentRetrievalFunction<HashMap<String, Object>, HashMap<String, Object>, HashMap<String, Object>> {

	private static final long serialVersionUID = 1L;

	private ArangoDBLookUpService lookUpService;

	public ArangoDBLookUpFunction(ArangoDBRetrievalConfigBean arangoDBRetrievalConfigBean, LinkedHashMap<String, AnvizentDataType> structure,
			LinkedHashMap<String, AnvizentDataType> newStructure, ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction errorHandlerFunction,
			JobDetails jobDetails) throws InvalidArgumentsException {
		super(arangoDBRetrievalConfigBean, null, structure, newStructure, null, accumulators, errorHandlerFunction, jobDetails);
	}

	@Override
	public BaseDBRetrievalRow<HashMap<String, Object>, HashMap<String, Object>> process(HashMap<String, Object> row)
			throws ValidationViolationException, RecordProcessingException, DataCorruptedException {
		HashMap<String, Object> newRow;
		boolean inserted = false;
		try {
			if (lookUpService == null) {
				lookUpService = new ArangoDBLookUpService((ArangoDBLookUpConfigBean) configBean);
			}

			HashMap<String, Object> newObject = lookUpService.selectFields(row);

			newRow = (HashMap<String, Object>) newObject.get(General.LOOKUP_RESULTING_ROW);
			if (newRow == null || newRow.isEmpty()) {
				newRow = lookUpService.insertFields();
				inserted = true;
			}

			return new DBRetrievalRow<HashMap<String, Object>, HashMap<String, Object>>(row, RowUtil.addElements(row, newRow, newStructure), inserted,
					(Boolean) newObject.get(General.LOOKUP_CACHED), 1, (Integer) newObject.get(General.LOOKEDUP_ROWS_COUNT));
		} catch (ImproperValidationException | SQLException | TimeoutException | IOException exception) {
			throw new RecordProcessingException(exception.getMessage(), exception);
		} catch (InvalidConfigValueException | ClassNotFoundException | UnderConstructionException exception) {
			throw new ValidationViolationException(exception.getMessage(), exception);
		} catch (UnsupportedCoerceException | InvalidSituationException | InvalidLookUpException exception) {
			throw new DataCorruptedException(exception);
		}
	}

	@Override
	protected BaseAnvizentErrorSetter<HashMap<String, Object>, HashMap<String, Object>> getBaseAnvizentErrorRowSetter() throws InvalidArgumentsException {
		return AnvizentErrorSetter.getDefaultInstance();
	}
}