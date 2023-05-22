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
import com.anvizent.elt.core.spark.mapping.config.bean.ReplicateConfigBean;
import com.anvizent.elt.core.spark.mapping.doc.helper.MappingDocHelper;
import com.anvizent.elt.core.spark.mapping.doc.helper.ReplicateDocHelper;
import com.anvizent.elt.core.spark.mapping.function.ReplicateMappingFunction;
import com.anvizent.elt.core.spark.mapping.schema.validator.MappingSchemaValidator;
import com.anvizent.elt.core.spark.mapping.validator.MappingValidator;
import com.anvizent.elt.core.spark.mapping.validator.ReplicateValidator;
import com.anvizent.elt.core.spark.util.StructureUtil;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ReplicateFactory extends MappingFactory {

	private static final long serialVersionUID = 1L;

	@Override
	public Component getComponent(ConfigBean configBean, MappingConfigBean mappingConfigBean, Component component, ArrayList<String> streamNames,
	        ErrorHandlerSink errorHandlerSink)
	        throws UnimplementedException, ImproperValidationException, InvalidRelationException, UnsupportedException, Exception {
		ReplicateConfigBean replicateConfigBean = (ReplicateConfigBean) mappingConfigBean;

		if (replicateConfigBean != null) {
			LinkedHashMap<String, AnvizentDataType> newStructure = getNewStructure(replicateConfigBean, component.getStructure());

			JavaRDD<HashMap<String, Object>> replicatedRDD = getReplicateRDD(0, streamNames, component, configBean, replicateConfigBean, newStructure,
			        errorHandlerSink);

			if (configBean.isPersist()) {
				replicatedRDD.persist(StorageLevel.MEMORY_AND_DISK());
			}

			Component finalComponent = Component.createComponent(component.getSparkSession(), component.getName(), streamNames.get(0), replicatedRDD,
			        newStructure);
			for (int i = 1; i < streamNames.size(); i++) {
				finalComponent.addRDD(streamNames.get(i),
				        getReplicateRDD(i, streamNames, component, configBean, replicateConfigBean, newStructure, errorHandlerSink));
			}

			return finalComponent;
		} else {
			return component;
		}
	}

	private JavaRDD<HashMap<String, Object>> getReplicateRDD(int index, ArrayList<String> streamNames, Component component, ConfigBean configBean,
	        ReplicateConfigBean replicateConfigBean, LinkedHashMap<String, AnvizentDataType> newStructure, ErrorHandlerSink errorHandlerSink)
	        throws UnimplementedException, ImproperValidationException, InvalidRelationException, UnsupportedException, Exception {
		String streamName = streamNames.size() > 1 ? "_" + streamNames.get(index) : "";

		JavaRDD<HashMap<String, Object>> replicatedRDD = component.getRDD(streamNames.get(index)).flatMap(new ReplicateMappingFunction(configBean,
		        replicateConfigBean, component.getStructure(), newStructure,
		        ApplicationBean.getInstance().getAccumulators(replicateConfigBean.getComponentName(), replicateConfigBean.getMappingConfigName() + streamName),
		        ErrorHandlerUtil.getErrorHandlerFunction(configBean, component.getStructure(), errorHandlerSink,
		                replicateConfigBean.getMappingConfigName() + streamName),
		        ErrorHandlerUtil.getJobDetails(configBean, replicateConfigBean.getMappingConfigName() + streamName)));

		return replicatedRDD;
	}

	private LinkedHashMap<String, AnvizentDataType> getNewStructure(ReplicateConfigBean replicateConfigBean, LinkedHashMap<String, AnvizentDataType> structure)
	        throws InvalidConfigException {
		ArrayList<AnvizentDataType> structureToAdd = new ArrayList<AnvizentDataType>();

		for (int i = 0; i < replicateConfigBean.getFields().size(); i++) {
			structureToAdd.add(structure.get(replicateConfigBean.getFields().get(i)));
		}

		return StructureUtil.getNewStructure(replicateConfigBean, structure, replicateConfigBean.getToFields(), structureToAdd,
		        replicateConfigBean.getPositions(), ConfigConstants.Mapping.Replicate.REPLICATE_TO_FIELDS,
		        ConfigConstants.Mapping.Replicate.REPLICATE_POSITIONS);
	}

	@Override
	public MappingValidator getMappingValidator() {
		return new ReplicateValidator();
	}

	@Override
	public MappingSchemaValidator getMappingSchemaValidator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MappingDocHelper getDocHelper() throws InvalidParameter {
		return new ReplicateDocHelper();
	}

	@Override
	public void createSpecialAccumulators(MappingConfigBean mappingConfigBean, ArrayList<String> streamNames, boolean componentLevelIn,
	        boolean componentLevelOut, StatsType statsType) {
		// TODO Auto-generated method stub
	}
}
