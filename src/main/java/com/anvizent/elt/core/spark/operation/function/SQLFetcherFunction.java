package com.anvizent.elt.core.spark.operation.function;

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
import com.anvizent.elt.core.spark.operation.config.bean.SQLFetcherConfigBean;
import com.anvizent.elt.core.spark.operation.config.bean.SQLRetrievalConfigBean;
import com.anvizent.elt.core.spark.operation.service.SQLFetcherService;
import com.anvizent.elt.core.spark.util.RowUtil;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
@SuppressWarnings("unchecked")
public class SQLFetcherFunction extends BaseAnvizentRetrievalFlatMapFunction<HashMap<String, Object>, HashMap<String, Object>> {

	private static final long serialVersionUID = 1L;

	private SQLFetcherService sqlFetcherService;

	public SQLFetcherFunction(SQLRetrievalConfigBean sqlRetrievalConfigBean, LinkedHashMap<String, AnvizentDataType> structure,
	        LinkedHashMap<String, AnvizentDataType> newStructure, ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction errorHandlerSinkFunction,
	        JobDetails jobDetails) throws InvalidArgumentsException {
		super(sqlRetrievalConfigBean, null, structure, newStructure, null, accumulators, errorHandlerSinkFunction, jobDetails);
	}

	@Override
	public BaseDBRetrievalRow<HashMap<String, Object>, Iterator<HashMap<String, Object>>> process(HashMap<String, Object> row)
	        throws RecordProcessingException, ValidationViolationException, DataCorruptedException {
		ArrayList<HashMap<String, Object>> newRows = new ArrayList<>();
		boolean inserted = false;
		try {
			if (sqlFetcherService == null) {
				sqlFetcherService = new SQLFetcherService((SQLFetcherConfigBean) configBean, structure, newStructure);
			}

			HashMap<String, Object> newObject = sqlFetcherService.selectFields(row);

			newRows = (ArrayList<HashMap<String, Object>>) newObject.get(General.LOOKUP_RESULTING_ROW);
			if (newRows == null || newRows.isEmpty()) {
				newRows = sqlFetcherService.insertFields(row);
				inserted = true;
			}

			return getMergedRows(row, newRows, inserted, (Boolean) newObject.get(General.LOOKUP_CACHED), (Integer) newObject.get(General.LOOKEDUP_ROWS_COUNT));
		} catch (SQLException | TimeoutException exception) {
			throw new RecordProcessingException(exception.getMessage(), exception);
		} catch (InvalidConfigValueException | ClassNotFoundException exception) {
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
