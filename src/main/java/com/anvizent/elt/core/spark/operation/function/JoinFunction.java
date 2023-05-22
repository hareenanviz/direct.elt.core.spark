package com.anvizent.elt.core.spark.operation.function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.lib.function.BaseAnvizentFunction;
import com.anvizent.elt.core.spark.operation.config.bean.JoinConfigBean;
import com.anvizent.elt.core.spark.operation.service.JoinService;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public abstract class JoinFunction<T> extends BaseAnvizentFunction<T, HashMap<String, Object>, HashMap<String, Object>> {

	private static final long serialVersionUID = 1L;

	protected JoinConfigBean joinConfigBean = (JoinConfigBean) configBean;
	protected final HashMap<String, String> lhsNewNames;
	protected final HashMap<String, String> rhsNewNames;

	public JoinFunction(JoinConfigBean joinConfigBean, LinkedHashMap<String, AnvizentDataType> structure, LinkedHashMap<String, AnvizentDataType> newStructure,
			HashMap<String, String> lhsNewNames, HashMap<String, String> rhsNewNames, ArrayList<AnvizentAccumulator> accumulators,
			AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails) throws InvalidArgumentsException {
		super(joinConfigBean, null, structure, newStructure, null, accumulators, errorHandlerSinkFunction, jobDetails);
		this.lhsNewNames = lhsNewNames;
		this.rhsNewNames = rhsNewNames;
	}

	@Override
	public HashMap<String, Object> process(T row) throws ValidationViolationException, RecordProcessingException {
		HashMap<String, Object>[] rows = getRows(row);

		return JoinService.buildNewRow(newStructure, rows[0], rows[1], joinConfigBean.getLHSPrefix(), joinConfigBean.getRHSPrefix(), lhsNewNames, rhsNewNames);
	}

	public abstract HashMap[] getRows(T row);
}
