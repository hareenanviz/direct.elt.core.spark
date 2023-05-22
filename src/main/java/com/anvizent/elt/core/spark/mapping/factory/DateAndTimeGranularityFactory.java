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
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.listener.common.constant.StatsType;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.common.util.ErrorHandlerUtil;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.mapping.config.bean.DateAndTimeGranularityConfigBean;
import com.anvizent.elt.core.spark.mapping.doc.helper.DateAndTimeGranularityDocHelper;
import com.anvizent.elt.core.spark.mapping.doc.helper.MappingDocHelper;
import com.anvizent.elt.core.spark.mapping.function.DateAndTimeGranularityMappingFunction;
import com.anvizent.elt.core.spark.mapping.schema.validator.MappingSchemaValidator;
import com.anvizent.elt.core.spark.mapping.validator.DateAndTimeGranularityValidator;
import com.anvizent.elt.core.spark.mapping.validator.MappingValidator;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class DateAndTimeGranularityFactory extends MappingFactory implements CleansingFactory {

	private static final long serialVersionUID = 1L;

	@Override
	public Component getComponent(ConfigBean configBean, MappingConfigBean mappingConfigBean, Component component, ArrayList<String> streamNames,
	        ErrorHandlerSink errorHandlerSink)
	        throws UnimplementedException, ImproperValidationException, InvalidRelationException, UnsupportedException, Exception {
		DateAndTimeGranularityConfigBean granularityConfigBean = (DateAndTimeGranularityConfigBean) mappingConfigBean;

		if (granularityConfigBean != null) {
			LinkedHashMap<String, AnvizentDataType> newStructure = new LinkedHashMap<>(component.getStructure());

			JavaRDD<HashMap<String, Object>> granularityRDD = getGranularityRDD(0, streamNames, component, configBean, granularityConfigBean, newStructure,
			        errorHandlerSink);

			if (configBean.isPersist()) {
				granularityRDD.persist(StorageLevel.MEMORY_AND_DISK());
			}

			Component finalComponent = Component.createComponent(component.getSparkSession(), component.getName(), streamNames.get(0), granularityRDD,
			        newStructure);
			for (int i = 1; i < streamNames.size(); i++) {
				finalComponent.addRDD(streamNames.get(i),
				        getGranularityRDD(i, streamNames, component, configBean, granularityConfigBean, newStructure, errorHandlerSink));
			}

			return finalComponent;
		} else {
			return component;
		}
	}

	private JavaRDD<HashMap<String, Object>> getGranularityRDD(int index, ArrayList<String> streamNames, Component component, ConfigBean configBean,
	        DateAndTimeGranularityConfigBean granularityConfigBean, LinkedHashMap<String, AnvizentDataType> newStructure, ErrorHandlerSink errorHandlerSink)
	        throws UnimplementedException, ImproperValidationException, InvalidRelationException, UnsupportedException, Exception {
		String streamName = streamNames.size() > 1 ? "_" + streamNames.get(index) : "";

		JavaRDD<HashMap<String, Object>> granularityRDD = component.getRDD(streamNames.get(index))
		        .flatMap(new DateAndTimeGranularityMappingFunction(configBean, granularityConfigBean, component.getStructure(), newStructure,
		                ApplicationBean.getInstance().getAccumulators(granularityConfigBean.getComponentName(),
		                        granularityConfigBean.getMappingConfigName() + streamName),
		                ErrorHandlerUtil.getErrorHandlerFunction(configBean, component.getStructure(), errorHandlerSink,
		                        granularityConfigBean.getMappingConfigName() + streamName),
		                ErrorHandlerUtil.getJobDetails(configBean, granularityConfigBean.getMappingConfigName() + streamName)));

		return granularityRDD;
	}

	@Override
	public MappingValidator getMappingValidator() {
		return new DateAndTimeGranularityValidator();
	}

	@Override
	public MappingSchemaValidator getMappingSchemaValidator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MappingDocHelper getDocHelper() throws InvalidParameter {
		return new DateAndTimeGranularityDocHelper();
	}

	@Override
	public void createSpecialAccumulators(MappingConfigBean mappingConfigBean, ArrayList<String> streamNames, boolean componentLevelIn,
	        boolean componentLevelOut, StatsType statsType) {
		// TODO Auto-generated method stub
	}
}
