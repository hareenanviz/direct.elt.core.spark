package com.anvizent.elt.core.spark.operation.function;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeoutException;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.exception.DataCorruptedException;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.InvalidConfigValueException;
import com.anvizent.elt.core.lib.exception.InvalidSituationException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.UnsupportedCoerceException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.lib.function.BaseAnvizentRetrievalFlatMapFunction;
import com.anvizent.elt.core.lib.row.formatter.AnvizentErrorSetter;
import com.anvizent.elt.core.lib.row.formatter.BaseAnvizentErrorSetter;
import com.anvizent.elt.core.lib.stats.beans.BaseDBRetrievalRow;
import com.anvizent.elt.core.lib.stats.beans.DBRetrievalRow;
import com.anvizent.elt.core.spark.constant.Constants.General;
import com.anvizent.elt.core.spark.exception.InvalidLookUpException;
import com.anvizent.elt.core.spark.operation.config.bean.ArangoDBFetcherConfigBean;
import com.anvizent.elt.core.spark.operation.config.bean.ArangoDBRetrievalConfigBean;
import com.anvizent.elt.core.spark.operation.service.ArangoDBFetcherService;
import com.anvizent.elt.core.spark.util.RowUtil;
import com.anvizent.query.builder.exception.UnderConstructionException;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
@SuppressWarnings("unchecked")
public class ArangoDBFetcherFunction extends BaseAnvizentRetrievalFlatMapFunction<HashMap<String, Object>, HashMap<String, Object>> {

	private static final long serialVersionUID = 1L;

	private ArangoDBFetcherService arangoDBFetcherService;

	public ArangoDBFetcherFunction(ArangoDBRetrievalConfigBean arangoDBRetrievalConfigBean, LinkedHashMap<String, AnvizentDataType> structure,
			LinkedHashMap<String, AnvizentDataType> newStructure, ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction errorHandlerFunction,
			JobDetails jobDetails) throws InvalidArgumentsException {
		super(arangoDBRetrievalConfigBean, null, structure, newStructure, null, accumulators, errorHandlerFunction, jobDetails);
	}

	@Override
	public BaseDBRetrievalRow<HashMap<String, Object>, Iterator<HashMap<String, Object>>> process(HashMap<String, Object> row)
			throws RecordProcessingException, ValidationViolationException, DataCorruptedException {
		ArrayList<HashMap<String, Object>> newRows = new ArrayList<>();
		boolean inserted = false;
		try {
			if (arangoDBFetcherService == null) {
				arangoDBFetcherService = new ArangoDBFetcherService((ArangoDBFetcherConfigBean) configBean);
			}

			HashMap<String, Object> newObject = arangoDBFetcherService.selectFields(row);

			newRows = (ArrayList<HashMap<String, Object>>) newObject.get(General.LOOKUP_RESULTING_ROW);
			if (newRows == null || newRows.isEmpty()) {
				newRows = arangoDBFetcherService.insertFields();
				inserted = true;
			}

			return getMergedRows(row, newRows, inserted, (Boolean) newObject.get(General.LOOKUP_CACHED), (Integer) newObject.get(General.LOOKEDUP_ROWS_COUNT));
		} catch (SQLException | TimeoutException | IOException exception) {
			throw new RecordProcessingException(exception.getMessage(), exception);
		} catch (InvalidConfigValueException | ClassNotFoundException | UnderConstructionException exception) {
			throw new ValidationViolationException(exception.getMessage(), exception);
		} catch (UnsupportedCoerceException | InvalidSituationException | InvalidLookUpException exception) {
			throw new DataCorruptedException(exception);
		}
	}

	private BaseDBRetrievalRow<HashMap<String, Object>, Iterator<HashMap<String, Object>>> getMergedRows(HashMap<String, Object> row,
			ArrayList<HashMap<String, Object>> newRows, boolean inserted, Boolean cached, Integer lookedUpRowsCount) {
		ArrayList<HashMap<String, Object>> mergedRows = new ArrayList<>();

		for (HashMap<String, Object> newRow : newRows) {
			mergedRows.add(RowUtil.addElements(row, newRow, newStructure));
		}

		return new DBRetrievalRow<HashMap<String, Object>, Iterator<HashMap<String, Object>>>(row, mergedRows.iterator(), inserted, cached, mergedRows.size(),
				lookedUpRowsCount);
	}

	@Override
	protected BaseAnvizentErrorSetter<HashMap<String, Object>, HashMap<String, Object>> getBaseAnvizentErrorRowSetter() throws InvalidArgumentsException {
		return AnvizentErrorSetter.getDefaultInstance();
	}
}
