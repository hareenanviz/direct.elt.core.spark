package com.anvizent.elt.core.spark.operation.function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.spark.api.java.Optional;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.lib.row.formatter.BaseAnvizentErrorSetter;
import com.anvizent.elt.core.spark.operation.config.bean.JoinConfigBean;
import com.anvizent.elt.core.spark.row.formatter.FullOuterJoinFunctionErrorSetter;

import scala.Tuple2;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class FullOuterJoinFunction extends JoinFunction<Tuple2<Optional<HashMap<String, Object>>, Optional<HashMap<String, Object>>>> {

	private static final long serialVersionUID = 1L;

	public FullOuterJoinFunction(JoinConfigBean joinConfigBean, LinkedHashMap<String, AnvizentDataType> structure,
			LinkedHashMap<String, AnvizentDataType> newStructure, HashMap<String, String> lhsNewNames, HashMap<String, String> rhsNewNames,
			ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails)
			throws InvalidArgumentsException {
		super(joinConfigBean, structure, newStructure, lhsNewNames, rhsNewNames, accumulators, errorHandlerSinkFunction, jobDetails);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public HashMap[] getRows(Tuple2<Optional<HashMap<String, Object>>, Optional<HashMap<String, Object>>> row) {
		HashMap[] rows = new HashMap[2];

		if (row._1().isPresent()) {
			rows[0] = row._1().get();
		} else {
			rows[0] = new LinkedHashMap<>();
		}

		if (row._2().isPresent()) {
			rows[1] = row._2().get();
		} else {
			rows[1] = new LinkedHashMap<>();
		}

		return rows;
	}

	@Override
	protected BaseAnvizentErrorSetter<Tuple2<Optional<HashMap<String, Object>>, Optional<HashMap<String, Object>>>, HashMap<String, Object>>
			getBaseAnvizentErrorRowSetter() throws InvalidArgumentsException {
		return new FullOuterJoinFunctionErrorSetter();
	}
}
