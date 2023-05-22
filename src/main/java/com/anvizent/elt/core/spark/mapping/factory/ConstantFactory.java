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
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.common.util.ErrorHandlerUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.mapping.config.bean.ConstantConfigBean;
import com.anvizent.elt.core.spark.mapping.doc.helper.ConstantsDocHelper;
import com.anvizent.elt.core.spark.mapping.doc.helper.MappingDocHelper;
import com.anvizent.elt.core.spark.mapping.function.ConstantMappingFunction;
import com.anvizent.elt.core.spark.mapping.schema.validator.MappingSchemaValidator;
import com.anvizent.elt.core.spark.mapping.validator.ConstantValidator;
import com.anvizent.elt.core.spark.mapping.validator.MappingValidator;
import com.anvizent.elt.core.spark.util.StructureUtil;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ConstantFactory extends MappingFactory {
	private static final long serialVersionUID = 1L;

	@Override
	public Component getComponent(ConfigBean configBean, MappingConfigBean mappingConfigBean, Component component, ArrayList<String> streamNames,
	        ErrorHandlerSink errorHandlerSink)
	        throws UnimplementedException, ImproperValidationException, InvalidRelationException, UnsupportedException, Exception {
		ConstantConfigBean constantConfigBean = (ConstantConfigBean) mappingConfigBean;

		if (constantConfigBean != null) {
			LinkedHashMap<String, AnvizentDataType> newStructure = getNewStructure(component, constantConfigBean);

			JavaRDD<HashMap<String, Object>> constantsAddedRDD = getConstantsAddedRDD(0, streamNames, component, configBean, constantConfigBean, newStructure,
			        errorHandlerSink);

			if (configBean.isPersist()) {
				constantsAddedRDD.persist(StorageLevel.MEMORY_AND_DISK());
			}

			Component finalComponent = Component.createComponent(component.getSparkSession(), component.getName(), streamNames.get(0), constantsAddedRDD,
			        newStructure);
			for (int i = 1; i < streamNames.size(); i++) {
				finalComponent.addRDD(streamNames.get(i),
				        getConstantsAddedRDD(i, streamNames, component, configBean, constantConfigBean, newStructure, errorHandlerSink));
			}

			return finalComponent;
		} else {
			return component;
		}
	}

	private JavaRDD<HashMap<String, Object>> getConstantsAddedRDD(int index, ArrayList<String> streamNames, Component component, ConfigBean configBean,
	        ConstantConfigBean constantConfigBean, LinkedHashMap<String, AnvizentDataType> newStructure, ErrorHandlerSink errorHandlerSink)
	        throws UnimplementedException, ImproperValidationException, InvalidRelationException, UnsupportedException, Exception {
		String streamName = streamNames.size() > 1 ? "_" + streamNames.get(index) : "";

		JavaRDD<HashMap<String, Object>> constantsAddedRDD = component.getRDD(streamNames.get(index)).flatMap(new ConstantMappingFunction(configBean,
		        constantConfigBean, component.getStructure(), newStructure,
		        ApplicationBean.getInstance().getAccumulators(constantConfigBean.getComponentName(), constantConfigBean.getMappingConfigName() + streamName),
		        ErrorHandlerUtil.getErrorHandlerFunction(configBean, component.getStructure(), errorHandlerSink,
		                constantConfigBean.getMappingConfigName() + streamName),
		        ErrorHandlerUtil.getJobDetails(configBean, constantConfigBean.getMappingConfigName() + streamName)));

		return constantsAddedRDD;
	}

	private LinkedHashMap<String, AnvizentDataType> getNewStructure(Component component, ConstantConfigBean constantConfigBean)
	        throws InvalidConfigException, UnsupportedException {
		ArrayList<AnvizentDataType> newConstansDataTypes = StructureUtil.getNewStructureDataTypes(constantConfigBean.getTypes(),
		        constantConfigBean.getPrecisions(), constantConfigBean.getScales());
		LinkedHashMap<String, AnvizentDataType> newStructure = StructureUtil.getNewStructure(constantConfigBean, component.getStructure(),
		        constantConfigBean.getFields(), newConstansDataTypes, constantConfigBean.getPositions(), ConfigConstants.Mapping.Constant.CONSTANT_FIELDS,
		        ConfigConstants.Mapping.Constant.CONSTANT_FIELDS_POSITIONS);

		return newStructure;
	}

	@Override
	public MappingValidator getMappingValidator() {
		return new ConstantValidator();
	}

	@Override
	public MappingSchemaValidator getMappingSchemaValidator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MappingDocHelper getDocHelper() throws InvalidParameter {
		return new ConstantsDocHelper();

	}

	@Override
	public void createSpecialAccumulators(MappingConfigBean mappingConfigBean, ArrayList<String> streamNames, boolean componentLevelIn,
	        boolean componentLevelOut, StatsType statsType) {
		// TODO Auto-generated method stub
	}
}
