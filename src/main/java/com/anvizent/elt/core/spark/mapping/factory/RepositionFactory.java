package com.anvizent.elt.core.spark.mapping.factory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.lib.function.EmptyFunction;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.listener.common.constant.StatsType;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.common.util.ErrorHandlerUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Mapping.Reposition;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.mapping.config.bean.RepositionConfigBean;
import com.anvizent.elt.core.spark.mapping.doc.helper.MappingDocHelper;
import com.anvizent.elt.core.spark.mapping.doc.helper.RepositionDocHelper;
import com.anvizent.elt.core.spark.mapping.schema.validator.MappingSchemaValidator;
import com.anvizent.elt.core.spark.mapping.validator.MappingValidator;
import com.anvizent.elt.core.spark.mapping.validator.RepositionValidator;
import com.anvizent.elt.core.spark.util.StructureUtil;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class RepositionFactory extends MappingFactory {

	private static final long serialVersionUID = 1L;

	@Override
	public Component getComponent(ConfigBean configBean, MappingConfigBean mappingConfigBean, Component component, ArrayList<String> streamNames,
	        ErrorHandlerSink errorHandlerSink)
	        throws UnimplementedException, ImproperValidationException, InvalidRelationException, UnsupportedException, Exception {
		RepositionConfigBean repositionConfigBean = (RepositionConfigBean) mappingConfigBean;

		if (repositionConfigBean != null) {
			validateFields(repositionConfigBean, component.getStructure());

			LinkedHashMap<String, AnvizentDataType> newStructure = getNewStructure(repositionConfigBean, component.getStructure());

			JavaRDD<HashMap<String, Object>> repositionRDD = getRepositionRDD(0, streamNames, component, configBean, repositionConfigBean, newStructure,
			        errorHandlerSink);

			if (configBean.isPersist()) {
				repositionRDD.persist(StorageLevel.MEMORY_AND_DISK());
			}

			Component finalComponent = Component.createComponent(component.getSparkSession(), component.getName(), streamNames.get(0), repositionRDD,
			        newStructure);
			for (int i = 1; i < streamNames.size(); i++) {
				finalComponent.addRDD(streamNames.get(i),
				        getRepositionRDD(i, streamNames, component, configBean, repositionConfigBean, newStructure, errorHandlerSink));
			}

			return finalComponent;
		} else {
			return component;
		}
	}

	private JavaRDD<HashMap<String, Object>> getRepositionRDD(int index, ArrayList<String> streamNames, Component component, ConfigBean configBean,
	        RepositionConfigBean repositionConfigBean, LinkedHashMap<String, AnvizentDataType> newStructure, ErrorHandlerSink errorHandlerSink)
	        throws UnimplementedException, ImproperValidationException, InvalidRelationException, UnsupportedException, Exception {
		String streamName = streamNames.size() > 1 ? "_" + streamNames.get(index) : "";

		JavaRDD<HashMap<String, Object>> repositionRDD = component.getRDD(streamNames.get(index))
		        .flatMap(new EmptyFunction(configBean, component.getStructure(), newStructure,
		                ApplicationBean.getInstance().getAccumulators(repositionConfigBean.getComponentName(),
		                        repositionConfigBean.getMappingConfigName() + streamName),
		                ErrorHandlerUtil.getErrorHandlerFunction(configBean, component.getStructure(), errorHandlerSink,
		                        repositionConfigBean.getMappingConfigName() + streamName),
		                ErrorHandlerUtil.getJobDetails(configBean, repositionConfigBean.getMappingConfigName() + streamName)));

		return repositionRDD;
	}

	private LinkedHashMap<String, AnvizentDataType> getNewStructure(RepositionConfigBean configBean, LinkedHashMap<String, AnvizentDataType> structure)
	        throws InvalidConfigException {
		return StructureUtil.getNewStructure(configBean, structure, configBean.getFields(), configBean.getPositions(), Reposition.FIELDS, Reposition.POSITIONS);
	}

	private void validateFields(RepositionConfigBean configBean, LinkedHashMap<String, AnvizentDataType> structure) throws InvalidConfigException {
		ArrayList<String> keys = new ArrayList<>(structure.keySet());
		InvalidConfigException invalidConfigException = new InvalidConfigException();

		invalidConfigException.setComponent(configBean.getFullConfigName());
		invalidConfigException.setComponentName(configBean.getComponentName());
		invalidConfigException.setSeekDetails(configBean.getSeekDetails());

		for (String field : configBean.getFields()) {
			if (!keys.contains(field)) {
				invalidConfigException.add("'" + field + "' is not available in the source field list.");
			}
		}

		if (invalidConfigException.getNumberOfExceptions() > 0) {
			throw invalidConfigException;
		}
	}

	@Override
	public MappingValidator getMappingValidator() {
		return new RepositionValidator();
	}

	@Override
	public MappingSchemaValidator getMappingSchemaValidator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MappingDocHelper getDocHelper() throws InvalidParameter {
		return new RepositionDocHelper();
	}

	@Override
	public void createSpecialAccumulators(MappingConfigBean mappingConfigBean, ArrayList<String> streamNames, boolean componentLevelIn,
	        boolean componentLevelOut, StatsType statsType) {
		// TODO Auto-generated method stub
	}
}
