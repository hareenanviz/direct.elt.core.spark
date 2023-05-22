package com.anvizent.elt.core.spark.operation.function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.exception.DataCorruptedException;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.lib.function.BaseAnvizentFunction;
import com.anvizent.elt.core.lib.row.formatter.BaseAnvizentErrorSetter;
import com.anvizent.elt.core.spark.operation.config.bean.SequenceConfigBean;
import com.anvizent.elt.core.spark.row.formatter.AnvizentZipperErrorSetter;
import com.anvizent.elt.core.spark.util.RowUtil;

import scala.Tuple2;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class SequenceFunction extends BaseAnvizentFunction<Tuple2<HashMap<String, Object>, Long>, HashMap<String, Object>, HashMap<String, Object>> {

	private static final long serialVersionUID = 1L;

	public SequenceFunction(ConfigBean configBean, LinkedHashMap<String, AnvizentDataType> structure, LinkedHashMap<String, AnvizentDataType> newStructure,
			ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails)
			throws InvalidArgumentsException {
		super(configBean, null, structure, newStructure, null, accumulators, errorHandlerSinkFunction, jobDetails);
	}

	@Override
	public HashMap<String, Object> process(Tuple2<HashMap<String, Object>, Long> tuple)
			throws ValidationViolationException, RecordProcessingException, DataCorruptedException {
		SequenceConfigBean sequenceConfigBean = (SequenceConfigBean) configBean;

		HashMap<String, Object> newData = new HashMap<>();

		for (int i = 0; i < sequenceConfigBean.getFields().size(); i++) {
			Long index = tuple._2();

			if (sequenceConfigBean.getInitialValues() != null) {
				index += sequenceConfigBean.getInitialValues().get(i);
			}

			newData.put(sequenceConfigBean.getFields().get(i), index);
		}

		return RowUtil.addElements(tuple._1(), newData, newStructure);
	}

	@Override
	protected BaseAnvizentErrorSetter<Tuple2<HashMap<String, Object>, Long>, HashMap<String, Object>> getBaseAnvizentErrorRowSetter()
			throws InvalidArgumentsException {
		return new AnvizentZipperErrorSetter();
	}
}
