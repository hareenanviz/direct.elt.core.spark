package com.anvizent.elt.core.spark.sink.factory;

import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.constant.Constants.StatsNames;
import com.anvizent.elt.core.lib.constant.StatsCategory;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.listener.common.constant.StatsType;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.constant.CounterType;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.schema.validator.SchemaValidator;

/**
 * @author Hareen Bejjanki
 *
 */
public abstract class SinkFactory extends Factory {
	private static final long serialVersionUID = 1L;

	public void processAndWrite(ConfigAndMappingConfigBeans configAndMappingConfigBeans, LinkedHashMap<String, Component> components, StatsType globalStatsType)
	        throws Exception {
		Component component = components.get(configAndMappingConfigBeans.getConfigBean().getSource());
		count(CounterType.READ, component);

		try {
			Component finalComponent = preProcess(configAndMappingConfigBeans, component, globalStatsType);
			SchemaValidator schemaValidator = getSchemaValidator();
			if (schemaValidator != null) {
				InvalidConfigException invalidConfigException = new InvalidConfigException();
				invalidConfigException.setDetails(configAndMappingConfigBeans.getConfigBean());
				schemaValidator.validate(configAndMappingConfigBeans.getConfigBean(), -1, finalComponent.getStructure(), invalidConfigException);
				if (invalidConfigException.getNumberOfExceptions() > 0) {
					throw invalidConfigException;
				}
			}

			write(configAndMappingConfigBeans.getConfigBean(), finalComponent, configAndMappingConfigBeans.getErrorHandlerSink());
		} catch (Exception exception) {
			throw exception;
			// TODO exception handling
		}
	}

	private Component preProcess(ConfigAndMappingConfigBeans configAndMappingConfigBeans, Component component, StatsType globalStatsType)
	        throws ImproperValidationException, UnimplementedException, InvalidConfigException, UnsupportedException, InvalidRelationException, Exception {
		return mapping(configAndMappingConfigBeans, configAndMappingConfigBeans.getStatsStore(), component,
		        configAndMappingConfigBeans.getConfigBean().getSourceStream(), globalStatsType);
	}

	@Override
	protected void createFactoryAccumulators(ConfigBean configBean, StatsType statsType, boolean componentLevel) {
		if ((statsType.equals(StatsType.COMPONENT_LEVEL) && !componentLevel) || statsType.equals(StatsType.ALL)) {
			AnvizentAccumulator inAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
			        getName(), StatsCategory.IN, StatsNames.IN, !componentLevel, getStatsCalculator(StatsCategory.IN, StatsNames.IN, getName()));

			ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName(), inAnvizentAccumulator);
		}

		AnvizentAccumulator writtenAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
		        getName(), StatsCategory.OUT, StatsNames.WRITTEN, true, getStatsCalculator(StatsCategory.OUT, StatsNames.WRITTEN, getName()));
		ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName(), writtenAnvizentAccumulator);
	}

	protected abstract void write(ConfigBean configBean, Component component, ErrorHandlerSink errorHandlerSink) throws Exception;
}
