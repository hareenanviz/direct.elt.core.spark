package com.anvizent.elt.core.spark.mapping.factory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;
import com.anvizent.elt.core.lib.constant.StatsCategory;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.lib.stats.calculator.ConditionalCleansingStatsCalculator;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.listener.common.constant.StatsType;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.common.util.ErrorHandlerUtil;
import com.anvizent.elt.core.spark.constant.CleansingValidationType;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.mapping.config.bean.ConditionalReplacementCleansingConfigBean;
import com.anvizent.elt.core.spark.mapping.doc.helper.ConditionalReplacementCleansingDocHelper;
import com.anvizent.elt.core.spark.mapping.doc.helper.MappingDocHelper;
import com.anvizent.elt.core.spark.mapping.function.ConditionalReplacementCleansingMappingFunction;
import com.anvizent.elt.core.spark.mapping.schema.validator.ConditionalReplacementCleansingSchemaValidator;
import com.anvizent.elt.core.spark.mapping.schema.validator.MappingSchemaValidator;
import com.anvizent.elt.core.spark.mapping.validator.ConditionalReplacementCleansingValidator;
import com.anvizent.elt.core.spark.mapping.validator.MappingValidator;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ConditionalReplacementCleansingFactory extends MappingFactory implements CleansingFactory {

	private static final long serialVersionUID = 1L;

	@Override
	public Component getComponent(ConfigBean configBean, MappingConfigBean mappingConfigBean, Component component, ArrayList<String> streamNames,
	        ErrorHandlerSink errorHandlerSink)
	        throws UnimplementedException, ImproperValidationException, InvalidRelationException, UnsupportedException, Exception {

		ConditionalReplacementCleansingConfigBean cleansingConfigBean = (ConditionalReplacementCleansingConfigBean) mappingConfigBean;

		if (cleansingConfigBean != null) {
			LinkedHashMap<String, AnvizentDataType> newStructure = new LinkedHashMap<>(component.getStructure());

			JavaRDD<HashMap<String, Object>> cleansingRDD = getCleansingRDD(0, streamNames, component, configBean, cleansingConfigBean, newStructure,
			        errorHandlerSink);

			if (configBean.isPersist()) {
				cleansingRDD.persist(StorageLevel.MEMORY_AND_DISK());
			}

			Component finalComponent = Component.createComponent(component.getSparkSession(), component.getName(), streamNames.get(0), cleansingRDD,
			        component.getStructure());
			for (int i = 1; i < streamNames.size(); i++) {
				finalComponent.addRDD(streamNames.get(i),
				        getCleansingRDD(i, streamNames, component, configBean, cleansingConfigBean, newStructure, errorHandlerSink));
			}

			return finalComponent;
		} else {
			return component;
		}
	}

	private JavaRDD<HashMap<String, Object>> getCleansingRDD(int index, ArrayList<String> streamNames, Component component, ConfigBean configBean,
	        ConditionalReplacementCleansingConfigBean cleansingConfigBean, LinkedHashMap<String, AnvizentDataType> newStructure,
	        ErrorHandlerSink errorHandlerSink)
	        throws UnimplementedException, ImproperValidationException, InvalidRelationException, UnsupportedException, Exception {
		String streamName = streamNames.size() > 1 ? "_" + streamNames.get(index) : "";

		JavaRDD<HashMap<String, Object>> cleansingRDD = component.getRDD(streamNames.get(index)).flatMap(new ConditionalReplacementCleansingMappingFunction(
		        configBean, cleansingConfigBean, component.getStructure(), newStructure,
		        ApplicationBean.getInstance().getAccumulators(cleansingConfigBean.getComponentName(), cleansingConfigBean.getMappingConfigName() + streamName),
		        ErrorHandlerUtil.getErrorHandlerFunction(configBean, component.getStructure(), errorHandlerSink,
		                cleansingConfigBean.getMappingConfigName() + streamName),
		        ErrorHandlerUtil.getJobDetails(configBean, cleansingConfigBean.getMappingConfigName() + streamName)));

		return cleansingRDD;
	}

	@Override
	public MappingValidator getMappingValidator() {
		return new ConditionalReplacementCleansingValidator();
	}

	@Override
	public MappingSchemaValidator getMappingSchemaValidator() {
		return new ConditionalReplacementCleansingSchemaValidator();
	}

	@Override
	public MappingDocHelper getDocHelper() throws InvalidParameter {
		return new ConditionalReplacementCleansingDocHelper();
	}

	@Override
	public void createSpecialAccumulators(MappingConfigBean mappingConfigBean, ArrayList<String> streamNames, boolean componentLevelIn,
	        boolean componentLevelOut, StatsType statsType) {
		if (mappingConfigBean == null || statsType == null || statsType.equals(StatsType.NONE) || statsType.equals(StatsType.COMPONENT_LEVEL)) {
			return;
		}

		ConditionalReplacementCleansingConfigBean configBean = (ConditionalReplacementCleansingConfigBean) mappingConfigBean;

		ArrayList<String> fields = configBean.getFields();
		ArrayList<CleansingValidationType> validationTypes = configBean.getValidationTypes();

		for (int j = 0; j < streamNames.size(); j++) {
			String streamName = streamNames.size() > 1 ? "_" + streamNames.get(j) : "";

			for (int i = 0; i < fields.size(); i++) {
				AnvizentAccumulator otherAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(),
				        mappingConfigBean.getComponentName(), mappingConfigBean.getMappingConfigName() + streamName, fields.get(i), StatsCategory.OTHER,
				        validationTypes.get(i).name(), false,
				        new ConditionalCleansingStatsCalculator(StatsCategory.OTHER, validationTypes.get(i).name(), fields.get(i)));

				ApplicationBean.getInstance().addAccumulator(mappingConfigBean.getComponentName(), mappingConfigBean.getMappingConfigName() + streamName,
				        otherAnvizentAccumulator);
			}
		}
	}
}
