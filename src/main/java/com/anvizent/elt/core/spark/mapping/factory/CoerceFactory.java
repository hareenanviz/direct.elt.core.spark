package com.anvizent.elt.core.spark.mapping.factory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

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
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.mapping.config.bean.CoerceConfigBean;
import com.anvizent.elt.core.spark.mapping.doc.helper.CoerceDocHelper;
import com.anvizent.elt.core.spark.mapping.doc.helper.MappingDocHelper;
import com.anvizent.elt.core.spark.mapping.function.CoerceMappingFunction;
import com.anvizent.elt.core.spark.mapping.schema.validator.MappingSchemaValidator;
import com.anvizent.elt.core.spark.mapping.validator.CoerceValidator;
import com.anvizent.elt.core.spark.mapping.validator.MappingValidator;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class CoerceFactory extends MappingFactory {

	private static final long serialVersionUID = 1L;

	@Override
	public Component getComponent(ConfigBean configBean, MappingConfigBean mappingConfigBean, Component component, ArrayList<String> streamNames,
	        ErrorHandlerSink errorHandlerSink)
	        throws UnimplementedException, ImproperValidationException, InvalidRelationException, UnsupportedException, Exception {
		CoerceConfigBean coerceConfigBean = (CoerceConfigBean) mappingConfigBean;

		if (coerceConfigBean != null) {
			LinkedHashMap<String, AnvizentDataType> newStructure = getNewCoerceStructure(component, coerceConfigBean);

			JavaRDD<HashMap<String, Object>> coercedRDD = getCoercedRDD(0, streamNames, component, configBean, coerceConfigBean, newStructure,
			        errorHandlerSink);

			if (configBean.isPersist()) {
				coercedRDD.persist(StorageLevel.MEMORY_AND_DISK());
			}

			Component finalComponent = Component.createComponent(component.getSparkSession(), component.getName(), streamNames.get(0), coercedRDD,
			        newStructure);
			for (int i = 1; i < streamNames.size(); i++) {
				finalComponent.addRDD(streamNames.get(i),
				        getCoercedRDD(i, streamNames, component, configBean, coerceConfigBean, newStructure, errorHandlerSink));
			}

			return finalComponent;
		} else {
			return component;
		}
	}

	private JavaRDD<HashMap<String, Object>> getCoercedRDD(int index, ArrayList<String> streamNames, Component component, ConfigBean configBean,
	        CoerceConfigBean coerceConfigBean, LinkedHashMap<String, AnvizentDataType> newStructure, ErrorHandlerSink errorHandlerSink)
	        throws UnimplementedException, ImproperValidationException, InvalidRelationException, UnsupportedException, Exception {
		String streamName = streamNames.size() > 1 ? "_" + streamNames.get(index) : "";

		JavaRDD<HashMap<String, Object>> coercedRDD = component.getRDD(streamNames.get(index)).flatMap(new CoerceMappingFunction(configBean, coerceConfigBean,
		        newStructure, component.getStructure(),
		        ApplicationBean.getInstance().getAccumulators(coerceConfigBean.getComponentName(), coerceConfigBean.getMappingConfigName() + streamName),
		        ErrorHandlerUtil.getErrorHandlerFunction(configBean, component.getStructure(), errorHandlerSink,
		                coerceConfigBean.getMappingConfigName() + streamName),
		        ErrorHandlerUtil.getJobDetails(configBean, coerceConfigBean.getMappingConfigName() + streamName)));

		return coercedRDD;
	}

	private LinkedHashMap<String, AnvizentDataType> getNewCoerceStructure(Component component, CoerceConfigBean coerceConfigBean) throws UnsupportedException {
		LinkedHashMap<String, AnvizentDataType> newStructure = getNewStructure(component);

		for (Entry<String, AnvizentDataType> newMap : newStructure.entrySet()) {
			for (int i = 0; i < coerceConfigBean.getFields().size(); i++) {
				String field = coerceConfigBean.getFields().get(i);
				if (field.equals(newMap.getKey())) {
					int precision = coerceConfigBean.getPrecisions() == null ? General.DECIMAL_PRECISION
					        : (coerceConfigBean.getPrecisions().get(i) == null ? General.DECIMAL_PRECISION : coerceConfigBean.getPrecisions().get(i));
					int scale = coerceConfigBean.getScales() == null ? General.DECIMAL_SCALE
					        : (coerceConfigBean.getScales().get(i) == null ? General.DECIMAL_SCALE : coerceConfigBean.getScales().get(i));
					newStructure.put(newMap.getKey(), new AnvizentDataType(coerceConfigBean.getTypes().get(i), precision, scale));
				}
			}
		}

		return newStructure;
	}

	private LinkedHashMap<String, AnvizentDataType> getNewStructure(Component component) {
		LinkedHashMap<String, AnvizentDataType> newStructure = new LinkedHashMap<String, AnvizentDataType>();
		for (Entry<String, AnvizentDataType> map : component.getStructure().entrySet()) {
			newStructure.put(map.getKey(), map.getValue());
		}
		return newStructure;
	}

	@Override
	public MappingValidator getMappingValidator() {
		return new CoerceValidator();
	}

	@Override
	public MappingSchemaValidator getMappingSchemaValidator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MappingDocHelper getDocHelper() throws InvalidParameter {
		return new CoerceDocHelper();
	}

	@Override
	public void createSpecialAccumulators(MappingConfigBean mappingConfigBean, ArrayList<String> streamNames, boolean componentLevelIn,
	        boolean componentLevelOut, StatsType statsType) {
		// TODO Auto-generated method stub
	}
}