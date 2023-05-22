package com.anvizent.elt.core.spark.sink.function;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeoutException;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.exception.DataCorruptedException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.lib.stats.beans.WrittenRow;
import com.anvizent.elt.core.spark.sink.config.bean.ArangoDBSinkConfigBean;
import com.anvizent.elt.core.spark.sink.service.ArangoDBSinkService;
import com.anvizent.elt.core.spark.sink.util.bean.ArangoDBSinkGetResult;
import com.anvizent.query.builder.exception.InvalidInputException;
import com.anvizent.query.builder.exception.UnderConstructionException;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public abstract class ArangoDBSinkFunction extends AnvizentVoidFunction {

	private static final long serialVersionUID = 1L;

	public ArangoDBSinkFunction(ConfigBean configBean, LinkedHashMap<String, AnvizentDataType> structure, LinkedHashMap<String, AnvizentDataType> newStructure,
			ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails) throws InvalidRelationException {
		super(configBean, null, structure, newStructure, null, accumulators, errorHandlerSinkFunction, jobDetails);
	}

	@Override
	public WrittenRow process(HashMap<String, Object> row) throws ValidationViolationException, RecordProcessingException, DataCorruptedException {
		try {
			String arangoDBKey = ArangoDBSinkService.getOrGenerateId((ArangoDBSinkConfigBean) configBean, row);
			boolean canAvoidSelect = ArangoDBSinkService.canAvoidSelect((ArangoDBSinkConfigBean) configBean, arangoDBKey);
			ArangoDBSinkGetResult arangoDBSinkGetResult =
					ArangoDBSinkService.getRethinkDBSinkGetResult((ArangoDBSinkConfigBean) configBean, row, arangoDBKey, canAvoidSelect);

			return process(row, arangoDBSinkGetResult);
		} catch (TimeoutException | SQLException | IOException exception) {
			throw new RecordProcessingException(exception.getMessage(), exception);
		} catch (ClassNotFoundException | UnderConstructionException | InvalidInputException | ParseException exception) {
			throw new ValidationViolationException(exception.getMessage(), exception);
		}
	}

	protected abstract WrittenRow process(HashMap<String, Object> row, ArangoDBSinkGetResult rethinkDBSinkGetResult)
			throws RecordProcessingException, DataCorruptedException, ValidationViolationException;
}
