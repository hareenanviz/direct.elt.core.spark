package com.anvizent.elt.core.spark.mapping.factory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.listener.common.constant.StatsType;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.common.util.ErrorHandlerUtil;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.mapping.config.bean.RetainConfigBean;
import com.anvizent.elt.core.spark.mapping.doc.helper.MappingDocHelper;
import com.anvizent.elt.core.spark.mapping.doc.helper.RetainDocHelper;
import com.anvizent.elt.core.spark.mapping.function.RetainMappingFunction;
import com.anvizent.elt.core.spark.mapping.schema.validator.MappingSchemaValidator;
import com.anvizent.elt.core.spark.mapping.validator.MappingValidator;
import com.anvizent.elt.core.spark.mapping.validator.RetainValidator;
import com.anvizent.elt.core.spark.util.StructureUtil;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class RetainFactory extends MappingFactory {

	private static final long serialVersionUID = 1L;

	@Override
	public Component getComponent(ConfigBean configBean, MappingConfigBean mappingConfigBean, Component component, ArrayList<String> streamNames,
	        ErrorHandlerSink errorHandlerSink)
	        throws UnimplementedException, ImproperValidationException, InvalidRelationException, UnsupportedException, Exception {
		RetainConfigBean retainConfigBean = (RetainConfigBean) mappingConfigBean;

		if (retainConfigBean != null) {
			LinkedHashMap<String, AnvizentDataType> newStructure = getNewRetainStructure(component, retainConfigBean);
			JavaRDD<HashMap<String, Object>> retainRDD = getRetainRDD(0, streamNames, component, configBean, retainConfigBean, newStructure, errorHandlerSink);

			if (configBean.isPersist()) {
				retainRDD.persist(StorageLevel.MEMORY_AND_DISK());
			}

			Component finalComponent = Component.createComponent(component.getSparkSession(), component.getName(), streamNames.get(0), retainRDD, newStructure);
			for (int i = 1; i < streamNames.size(); i++) {
				finalComponent.addRDD(streamNames.get(i),
				        getRetainRDD(i, streamNames, component, configBean, retainConfigBean, newStructure, errorHandlerSink));
			}

			return finalComponent;
		} else {
			return component;
		}
	}

	private JavaRDD<HashMap<String, Object>> getRetainRDD(int index, ArrayList<String> streamNames, Component component, ConfigBean configBean,
	        RetainConfigBean retainConfigBean, LinkedHashMap<String, AnvizentDataType> newStructure, ErrorHandlerSink errorHandlerSink)
	        throws UnimplementedException, ImproperValidationException, InvalidRelationException, UnsupportedException, Exception {
		String streamName = streamNames.size() > 1 ? "_" + streamNames.get(index) : "";

		JavaRDD<HashMap<String, Object>> retainRDD = component.getRDD(streamNames.get(index)).flatMap(new RetainMappingFunction(configBean, retainConfigBean,
		        component.getStructure(), newStructure,
		        ApplicationBean.getInstance().getAccumulators(retainConfigBean.getComponentName(), retainConfigBean.getMappingConfigName() + streamName),
		        ErrorHandlerUtil.getErrorHandlerFunction(configBean, component.getStructure(), errorHandlerSink,
		                retainConfigBean.getMappingConfigName() + streamName),
		        ErrorHandlerUtil.getJobDetails(configBean, retainConfigBean.getMappingConfigName() + streamName)));

		if (configBean.isPersist()) {
			retainRDD.persist(StorageLevel.MEMORY_AND_DISK());
		}

		return retainRDD;
	}

	private LinkedHashMap<String, AnvizentDataType> getNewRetainStructure(Component component, RetainConfigBean retainConfigBean) {
		if (retainConfigBean.getRetainFields() == null && retainConfigBean.getEmitFields() != null) {
			return getRetainedStructure(retainConfigBean.getEmitFields(), component.getStructure());
		} else if (retainConfigBean.getEmitFields() == null && retainConfigBean.getRetainFields() != null) {
			return getRetainedStructure(component.getStructure(), retainConfigBean);
		} else {
			return component.getStructure();
		}
	}

	private LinkedHashMap<String, AnvizentDataType> getRetainedStructure(LinkedHashMap<String, AnvizentDataType> structure, RetainConfigBean retainConfigBean) {
		ArrayList<String> keySet = new ArrayList<>(structure.keySet());
		LinkedHashMap<String, AnvizentDataType> newStructure = new LinkedHashMap<>();

		for (int i = 0; i < retainConfigBean.getRetainFields().size(); i++) {
			String retainAs = retainConfigBean.getRetainFields().get(i);

			if (keySet.contains(retainAs)) {
				if (retainConfigBean.getRetainFieldsAs() != null) {
					retainAs = retainConfigBean.getRetainFieldsAs().get(i) == null || retainConfigBean.getRetainFieldsAs().get(i).isEmpty()
					        ? retainConfigBean.getRetainFields().get(i)
					        : retainConfigBean.getRetainFieldsAs().get(i);
				}

				newStructure.put(retainAs, structure.get(retainConfigBean.getRetainFields().get(i)));
			}
		}

		return retainAt(newStructure, retainConfigBean);
	}

	private LinkedHashMap<String, AnvizentDataType> retainAt(LinkedHashMap<String, AnvizentDataType> newStructure, RetainConfigBean retainConfigBean) {
		if (retainConfigBean.getRetainFieldsAt() != null) {
			LinkedHashMap<String, AnvizentDataType> rePositionedStructure = new LinkedHashMap<>();
			ArrayList<String> finalRetainFields = new ArrayList<>(newStructure.keySet());

			TreeMap<Integer, Integer> indexes = StructureUtil.getSortedIndexes(retainConfigBean.getRetainFieldsAt(), retainConfigBean.getRetainFields().size(),
			        retainConfigBean.getRetainFields().size(), false);

			for (Entry<Integer, Integer> entry : indexes.entrySet()) {
				int value = entry.getValue();
				rePositionedStructure.put(finalRetainFields.get(value), newStructure.get(finalRetainFields.get(value)));
			}

			return rePositionedStructure;
		} else {
			return newStructure;
		}
	}

	private LinkedHashMap<String, AnvizentDataType> getRetainedStructure(ArrayList<String> emitFields, LinkedHashMap<String, AnvizentDataType> structure) {
		ArrayList<String> keySet = new ArrayList<>(structure.keySet());
		for (int i = 0; i < emitFields.size(); i++) {
			if (keySet.contains(emitFields.get(i))) {
				structure.remove(emitFields.get(i));
			}
		}

		return structure;
	}

	@Override
	public MappingValidator getMappingValidator() {
		return new RetainValidator();
	}

	@Override
	public MappingSchemaValidator getMappingSchemaValidator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MappingDocHelper getDocHelper() throws InvalidParameter {
		return new RetainDocHelper();
	}

	@Override
	public void createSpecialAccumulators(MappingConfigBean mappingConfigBean, ArrayList<String> streamNames, boolean componentLevelIn,
	        boolean componentLevelOut, StatsType statsType) {
		// TODO Auto-generated method stub
	}
}