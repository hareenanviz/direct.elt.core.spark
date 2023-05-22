package com.anvizent.elt.core.spark.mapping.factory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;
import com.anvizent.elt.core.lib.constant.Constants.StatsNames;
import com.anvizent.elt.core.lib.constant.StatsCategory;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.lib.stats.calculator.AnvizentStatsCalculator;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.listener.common.constant.StatsType;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.mapping.doc.helper.MappingDocHelper;
import com.anvizent.elt.core.spark.mapping.schema.validator.MappingSchemaValidator;
import com.anvizent.elt.core.spark.mapping.validator.MappingValidator;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public abstract class MappingFactory implements Serializable {
	private static final long serialVersionUID = 1L;

	public abstract Component getComponent(ConfigBean configBean, MappingConfigBean mappingConfigBean, Component component, ArrayList<String> streamNames,
	        ErrorHandlerSink errorHandlerSink)
	        throws UnsupportedException, ImproperValidationException, InvalidConfigException, UnimplementedException, InvalidRelationException, Exception;

	public abstract MappingValidator getMappingValidator();

	public abstract MappingSchemaValidator getMappingSchemaValidator();

	public abstract MappingDocHelper getDocHelper() throws InvalidParameter;

	public void createMappingAccumulators(MappingConfigBean mappingConfigBean, ArrayList<String> streamNames, boolean componentLevelIn,
	        boolean componentLevelOut, StatsType statsType) {
		if (mappingConfigBean == null || this instanceof ConditionalReplacementCleansingFactory || statsType == null || statsType.equals(StatsType.NONE)) {
			return;
		}

		for (int i = 0; i < streamNames.size(); i++) {
			String streamName = streamNames.size() > 1 ? "_" + streamNames.get(i) : "";

			if (componentLevelIn || statsType.equals(StatsType.ALL)) {
				AnvizentAccumulator inAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(),
				        mappingConfigBean.getComponentName(), mappingConfigBean.getMappingConfigName() + streamName, StatsCategory.IN, StatsNames.IN,
				        componentLevelIn,
				        new AnvizentStatsCalculator<LinkedHashMap<String, Object>, LinkedHashMap<String, Object>>(StatsCategory.IN, StatsNames.IN));

				ApplicationBean.getInstance().addAccumulator(mappingConfigBean.getComponentName(), mappingConfigBean.getMappingConfigName() + streamName,
				        inAnvizentAccumulator);
			}

			if (componentLevelOut || statsType.equals(StatsType.ALL)) {
				AnvizentAccumulator outAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(),
				        mappingConfigBean.getComponentName(), mappingConfigBean.getMappingConfigName() + streamName, StatsCategory.OUT, StatsNames.OUT,
				        componentLevelOut,
				        new AnvizentStatsCalculator<LinkedHashMap<String, Object>, LinkedHashMap<String, Object>>(StatsCategory.OUT, StatsNames.OUT));

				ApplicationBean.getInstance().addAccumulator(mappingConfigBean.getComponentName(), mappingConfigBean.getMappingConfigName() + streamName,
				        outAnvizentAccumulator);
			}

			AnvizentAccumulator errorAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(),
			        mappingConfigBean.getComponentName(), mappingConfigBean.getMappingConfigName() + streamName, StatsCategory.ERROR, StatsNames.ERROR, true,
			        new AnvizentStatsCalculator<LinkedHashMap<String, Object>, LinkedHashMap<String, Object>>(StatsCategory.ERROR, StatsNames.ERROR));

			ApplicationBean.getInstance().addAccumulator(mappingConfigBean.getComponentName(), mappingConfigBean.getMappingConfigName() + streamName,
			        errorAnvizentAccumulator);
		}
	}

	public abstract void createSpecialAccumulators(MappingConfigBean mappingConfigBean, ArrayList<String> emitStreams, boolean componentLevelIn,
	        boolean componentLevelOut, StatsType statsType);
}