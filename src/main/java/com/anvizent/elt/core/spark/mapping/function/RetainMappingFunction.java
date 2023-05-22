package com.anvizent.elt.core.spark.mapping.function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

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
import com.anvizent.elt.core.spark.mapping.config.bean.RetainConfigBean;

/**
 * @author Hareen Bejjanki
 * @author apurva.deshmukh -- Either retains or emits given fields
 *
 */
public class RetainMappingFunction extends AnvizentFunction implements MappingFunction {

	private static final long serialVersionUID = 1L;

	public RetainMappingFunction(ConfigBean configBean, MappingConfigBean mappingConfigBean, LinkedHashMap<String, AnvizentDataType> structure,
			LinkedHashMap<String, AnvizentDataType> newStructure, ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction errorHandlerSinkFunction,
			JobDetails jobDetails) throws InvalidArgumentsException {
		super(configBean, mappingConfigBean, structure, newStructure, null, accumulators, errorHandlerSinkFunction, jobDetails);
	}

	@Override
	public HashMap<String, Object> process(HashMap<String, Object> row) throws ValidationViolationException, RecordProcessingException {
		RetainConfigBean retainConfigBean = (RetainConfigBean) mappingConfigBean;

		if (retainConfigBean.getRetainFields() == null && retainConfigBean.getEmitFields() != null) {
			return emitFields(row);
		} else if (retainConfigBean.getRetainFields() != null && retainConfigBean.getEmitFields() == null) {
			return retainFields(row);
		} else {
			return row;
		}
	}

	private HashMap<String, Object> retainFields(HashMap<String, Object> row) {
		RetainConfigBean retainConfigBean = (RetainConfigBean) mappingConfigBean;
		HashMap<String, Object> newRow = new HashMap<>();

		for (Entry<String, AnvizentDataType> entry : newStructure.entrySet()) {
			String key = entry.getKey();

			if (retainConfigBean.getRetainFieldsAs() != null) {
				int index = retainConfigBean.getRetainFieldsAs().indexOf(entry.getKey());
				if (index != -1) {
					key = retainConfigBean.getRetainFields().get(index);
				}
			}

			newRow.put(entry.getKey(), row.get(key));
		}

		return newRow;
	}

	private HashMap<String, Object> emitFields(HashMap<String, Object> row) {
		RetainConfigBean retainConfigBean = (RetainConfigBean) mappingConfigBean;
		HashMap<String, Object> newRow = new HashMap<>(row);
		ArrayList<String> keySet = new ArrayList<>(row.keySet());

		for (int i = 0; i < retainConfigBean.getEmitFields().size(); i++) {
			if (keySet.contains(retainConfigBean.getEmitFields().get(i))) {
				newRow.remove(retainConfigBean.getEmitFields().get(i));
			}
		}

		return newRow;
	}

}
