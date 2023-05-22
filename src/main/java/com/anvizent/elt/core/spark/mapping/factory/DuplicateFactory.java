package com.anvizent.elt.core.spark.mapping.factory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
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
import com.anvizent.elt.core.spark.constant.AppendAt;
import com.anvizent.elt.core.spark.constant.Constants;
import com.anvizent.elt.core.spark.constant.Constants.General;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.mapping.config.bean.DuplicateConfigBean;
import com.anvizent.elt.core.spark.mapping.doc.helper.DuplicateDocHelper;
import com.anvizent.elt.core.spark.mapping.doc.helper.MappingDocHelper;
import com.anvizent.elt.core.spark.mapping.function.DuplicateMappingFunction;
import com.anvizent.elt.core.spark.mapping.schema.validator.MappingSchemaValidator;
import com.anvizent.elt.core.spark.mapping.service.DuplicateService;
import com.anvizent.elt.core.spark.mapping.validator.DuplicateValidator;
import com.anvizent.elt.core.spark.mapping.validator.MappingValidator;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class DuplicateFactory extends MappingFactory {

	private static final long serialVersionUID = 1L;

	@Override
	public Component getComponent(ConfigBean configBean, MappingConfigBean mappingConfigBean, Component component, ArrayList<String> streamNames,
	        ErrorHandlerSink errorHandlerSink)
	        throws UnimplementedException, ImproperValidationException, InvalidRelationException, UnsupportedException, Exception {
		DuplicateConfigBean duplicateConfigBean = (DuplicateConfigBean) mappingConfigBean;

		if (duplicateConfigBean != null && duplicateConfigBean.isEveryMeasureAsString()) {
			return duplicatePrimitivesToString(configBean, duplicateConfigBean, component, streamNames, errorHandlerSink);
		} else {
			return component;
		}
	}

	private Component duplicatePrimitivesToString(ConfigBean configBean, DuplicateConfigBean duplicateConfigBean, Component component,
	        ArrayList<String> streamNames, ErrorHandlerSink errorHandlerSink)
	        throws UnimplementedException, ImproperValidationException, InvalidRelationException, UnsupportedException, Exception {
		LinkedHashMap<String, AnvizentDataType> newStructure = getNewStructure(component.getStructure(), duplicateConfigBean);

		JavaRDD<HashMap<String, Object>> formattedRDD = getDuplicateRDD(0, streamNames, component, configBean, duplicateConfigBean, newStructure,
		        errorHandlerSink);

		if (configBean.isPersist()) {
			formattedRDD.persist(StorageLevel.MEMORY_AND_DISK());
		}

		Component finalComponent = Component.createComponent(component.getSparkSession(), component.getName(), streamNames.get(0), formattedRDD, newStructure);
		for (int i = 1; i < streamNames.size(); i++) {
			finalComponent.addRDD(streamNames.get(i),
			        getDuplicateRDD(i, streamNames, component, configBean, duplicateConfigBean, newStructure, errorHandlerSink));
		}

		return finalComponent;
	}

	private JavaRDD<HashMap<String, Object>> getDuplicateRDD(int index, ArrayList<String> streamNames, Component component, ConfigBean configBean,
	        DuplicateConfigBean duplicateConfigBean, LinkedHashMap<String, AnvizentDataType> newStructure, ErrorHandlerSink errorHandlerSink)
	        throws UnimplementedException, ImproperValidationException, InvalidRelationException, UnsupportedException, Exception {
		String streamName = streamNames.size() > 1 ? "_" + streamNames.get(index) : "";

		JavaRDD<HashMap<String, Object>> formattedRDD = component.getRDD(streamNames.get(index)).flatMap(new DuplicateMappingFunction(configBean,
		        duplicateConfigBean, component.getStructure(), newStructure,
		        ApplicationBean.getInstance().getAccumulators(duplicateConfigBean.getComponentName(), duplicateConfigBean.getMappingConfigName() + streamName),
		        ErrorHandlerUtil.getErrorHandlerFunction(configBean, component.getStructure(), errorHandlerSink,
		                duplicateConfigBean.getMappingConfigName() + streamName),
		        ErrorHandlerUtil.getJobDetails(configBean, duplicateConfigBean.getMappingConfigName() + streamName)));

		return formattedRDD;
	}

	public LinkedHashMap<String, AnvizentDataType> getNewStructure(LinkedHashMap<String, AnvizentDataType> structure, DuplicateConfigBean duplicateConfigBean)
	        throws UnsupportedException {
		String newKey = DuplicateService.getKey(duplicateConfigBean.getPrefix(), duplicateConfigBean.getSuffix());
		LinkedHashMap<String, AnvizentDataType> newStructure = new LinkedHashMap<String, AnvizentDataType>();
		LinkedHashMap<String, AnvizentDataType> appendAtEndMap = new LinkedHashMap<String, AnvizentDataType>();
		ArrayList<String> appendAtBeginKeySet = new ArrayList<String>();

		duplicateStructure(structure, duplicateConfigBean.getAppendAt(), newKey, newStructure, appendAtEndMap, appendAtBeginKeySet);

		if (duplicateConfigBean.getAppendAt().equals(AppendAt.END)) {
			return appendAtEndOfNewRow(appendAtEndMap, newStructure);
		} else if (duplicateConfigBean.getAppendAt().equals(AppendAt.BEGIN)) {
			return getBeginElementsMap(newKey, appendAtBeginKeySet, structure, newStructure);
		} else {
			return newStructure;
		}
	}

	@SuppressWarnings("rawtypes")
	private void duplicateStructure(LinkedHashMap<String, AnvizentDataType> structure, AppendAt appendAt, String newKey,
	        LinkedHashMap<String, AnvizentDataType> newStructure, LinkedHashMap<String, AnvizentDataType> appendAtEndMap, ArrayList<String> appendAtBeginKeySet)
	        throws UnsupportedException {
		for (Entry<String, AnvizentDataType> map : structure.entrySet()) {
			Class javaType = map.getValue().getJavaType();
			if (!Constants.Type.DIMENSIONS.contains(javaType)) {
				addToNewStructure(appendAt, newStructure, StringUtils.replace(newKey, General.PLACEHOLDER, map.getKey()), map, appendAtEndMap,
				        appendAtBeginKeySet);
			} else {
				newStructure.put(map.getKey(), map.getValue());
			}
		}
	}

	private static void addToNewStructure(AppendAt appendAt, LinkedHashMap<String, AnvizentDataType> newStructure, String newKey,
	        Entry<String, AnvizentDataType> map, LinkedHashMap<String, AnvizentDataType> appendAtEndMap, ArrayList<String> appendAtBeginKeySet)
	        throws UnsupportedException {
		if (appendAt.equals(AppendAt.BEFORE)) {
			newStructure.put(newKey, new AnvizentDataType(String.class));
			newStructure.put(map.getKey(), map.getValue());
		} else if (appendAt.equals(AppendAt.NEXT)) {
			newStructure.put(map.getKey(), map.getValue());
			newStructure.put(newKey, new AnvizentDataType(String.class));
		} else if (appendAt.equals(AppendAt.END)) {
			newStructure.put(map.getKey(), map.getValue());
			appendAtEndMap.put(newKey, new AnvizentDataType(String.class));
		} else {
			newStructure.put(map.getKey(), map.getValue());
			appendAtBeginKeySet.add(map.getKey());
		}
	}

	private static LinkedHashMap<String, AnvizentDataType> appendAtEndOfNewRow(LinkedHashMap<String, AnvizentDataType> appendAtEndMap,
	        LinkedHashMap<String, AnvizentDataType> newStructure) {
		for (Entry<String, AnvizentDataType> map : appendAtEndMap.entrySet()) {
			newStructure.put(map.getKey(), map.getValue());
		}

		return newStructure;
	}

	private static LinkedHashMap<String, AnvizentDataType> getBeginElementsMap(String newKey, ArrayList<String> appendAtBeginKeySet,
	        LinkedHashMap<String, AnvizentDataType> structure, LinkedHashMap<String, AnvizentDataType> newStructure) {
		LinkedHashMap<String, AnvizentDataType> newRowFromBegin = new LinkedHashMap<String, AnvizentDataType>();

		for (String beginKey : appendAtBeginKeySet) {
			newRowFromBegin.put(StringUtils.replace(newKey, General.PLACEHOLDER, beginKey), structure.get(beginKey));
		}
		newRowFromBegin.putAll(newStructure);

		return newRowFromBegin;
	}

	@Override
	public MappingValidator getMappingValidator() {
		return new DuplicateValidator();
	}

	@Override
	public MappingSchemaValidator getMappingSchemaValidator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MappingDocHelper getDocHelper() throws InvalidParameter {
		return new DuplicateDocHelper();
	}

	@Override
	public void createSpecialAccumulators(MappingConfigBean mappingConfigBean, ArrayList<String> streamNames, boolean componentLevelIn,
	        boolean componentLevelOut, StatsType statsType) {
		// TODO Auto-generated method stub
	}
}
