package com.anvizent.elt.core.spark.mapping.function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.lib.function.AnvizentFunction;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.lib.function.MappingFunction;
import com.anvizent.elt.core.spark.mapping.config.bean.ReplicateConfigBean;
import com.anvizent.elt.core.spark.util.RowUtil;

/**
 * @author Hareen Bejjanki
 * @author apurva.deshmukh -- replicates provided fields at given positions.
 *
 */
public class ReplicateMappingFunction extends AnvizentFunction implements MappingFunction {

	private static final long serialVersionUID = 1L;

	public ReplicateMappingFunction(ConfigBean configBean, MappingConfigBean mappingConfigBean, LinkedHashMap<String, AnvizentDataType> structure,
			LinkedHashMap<String, AnvizentDataType> newStructure, ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction errorHandlerSinkFunction,
			JobDetails jobDetails) throws InvalidArgumentsException {
		super(configBean, mappingConfigBean, structure, newStructure, null, accumulators, errorHandlerSinkFunction, jobDetails);
	}

	@Override
	public HashMap<String, Object> process(HashMap<String, Object> row) throws ValidationViolationException, RecordProcessingException {
		HashMap<String, Object> newRow = new HashMap<String, Object>();

		replicate(newRow, row);

		return RowUtil.addElements(row, newRow, newStructure);
	}

	private void replicate(HashMap<String, Object> newRow, HashMap<String, Object> row) {
		ReplicateConfigBean replicateConfigBean = (ReplicateConfigBean) mappingConfigBean;

		for (int i = 0; i < replicateConfigBean.getFields().size(); i++) {
			if (row.containsKey(replicateConfigBean.getFields().get(i))) {
				newRow.put(replicateConfigBean.getToFields().get(i), row.get(replicateConfigBean.getFields().get(i)));
			}
		}
	}
}