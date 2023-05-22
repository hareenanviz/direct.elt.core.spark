
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
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.mapping.config.bean.RenameConfigBean;
import com.anvizent.elt.core.spark.mapping.doc.helper.MappingDocHelper;
import com.anvizent.elt.core.spark.mapping.doc.helper.RenameDocHelper;
import com.anvizent.elt.core.spark.mapping.function.RenameMappingFunction;
import com.anvizent.elt.core.spark.mapping.schema.validator.MappingSchemaValidator;
import com.anvizent.elt.core.spark.mapping.validator.MappingValidator;
import com.anvizent.elt.core.spark.mapping.validator.RenameValidator;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class RenameFactory extends MappingFactory {

	private static final long serialVersionUID = 1L;

	@Override
	public Component getComponent(ConfigBean configBean, MappingConfigBean mappingConfigBean, Component component, ArrayList<String> streamNames,
	        ErrorHandlerSink errorHandlerSink)
	        throws UnimplementedException, ImproperValidationException, InvalidRelationException, UnsupportedException, Exception {
		RenameConfigBean renameConfigBean = (RenameConfigBean) mappingConfigBean;

		if (renameConfigBean != null) {
			LinkedHashMap<String, AnvizentDataType> newStructure = getRenamedStructure(renameConfigBean, component.getStructure());

			JavaRDD<HashMap<String, Object>> reNamedRDD = getRenamedRDD(0, streamNames, component, configBean, renameConfigBean, newStructure,
			        errorHandlerSink);

			if (configBean.isPersist()) {
				reNamedRDD.persist(StorageLevel.MEMORY_AND_DISK());
			}

			Component finalComponent = Component.createComponent(component.getSparkSession(), component.getName(), streamNames.get(0), reNamedRDD,
			        newStructure);
			for (int i = 1; i < streamNames.size(); i++) {
				finalComponent.addRDD(streamNames.get(i),
				        getRenamedRDD(i, streamNames, component, configBean, renameConfigBean, newStructure, errorHandlerSink));
			}

			return finalComponent;
		} else {
			return component;
		}
	}

	private JavaRDD<HashMap<String, Object>> getRenamedRDD(int index, ArrayList<String> streamNames, Component component, ConfigBean configBean,
	        RenameConfigBean renameConfigBean, LinkedHashMap<String, AnvizentDataType> newStructure, ErrorHandlerSink errorHandlerSink)
	        throws UnimplementedException, ImproperValidationException, InvalidRelationException, UnsupportedException, Exception {
		String streamName = streamNames.size() > 1 ? "_" + streamNames.get(index) : "";

		JavaRDD<HashMap<String, Object>> reNamedRDD = component.getRDD(streamNames.get(index)).flatMap(new RenameMappingFunction(configBean, renameConfigBean,
		        component.getStructure(), newStructure,
		        ApplicationBean.getInstance().getAccumulators(renameConfigBean.getComponentName(), renameConfigBean.getMappingConfigName() + streamName),
		        ErrorHandlerUtil.getErrorHandlerFunction(configBean, component.getStructure(), errorHandlerSink,
		                renameConfigBean.getMappingConfigName() + streamName),
		        ErrorHandlerUtil.getJobDetails(configBean, renameConfigBean.getMappingConfigName() + streamName)));

		return reNamedRDD;
	}

	private LinkedHashMap<String, AnvizentDataType> getRenamedStructure(RenameConfigBean renameConfigBean, LinkedHashMap<String, AnvizentDataType> structure) {

		LinkedHashMap<String, AnvizentDataType> newStructure = new LinkedHashMap<String, AnvizentDataType>();
		for (Entry<String, AnvizentDataType> entry : structure.entrySet()) {
			int index = renameConfigBean.getRenameFrom().indexOf(entry.getKey());
			if (index != -1) {
				newStructure.put(renameConfigBean.getRenameTo().get(index), entry.getValue());
			} else {
				newStructure.put(entry.getKey(), entry.getValue());
			}
		}

		return newStructure;
	}

	@Override
	public MappingValidator getMappingValidator() {
		return new RenameValidator();
	}

	@Override
	public MappingSchemaValidator getMappingSchemaValidator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MappingDocHelper getDocHelper() throws InvalidParameter {
		return new RenameDocHelper();
	}

	@Override
	public void createSpecialAccumulators(MappingConfigBean mappingConfigBean, ArrayList<String> streamNames, boolean componentLevelIn,
	        boolean componentLevelOut, StatsType statsType) {
		// TODO Auto-generated method stub
	}
}
