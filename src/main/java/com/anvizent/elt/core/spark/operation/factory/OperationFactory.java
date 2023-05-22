package com.anvizent.elt.core.spark.operation.factory;

import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.listener.common.constant.StatsType;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.constant.CounterType;
import com.anvizent.elt.core.spark.factory.Factory;

/**
 * @author Hareen Bejjanki
 *
 */
public abstract class OperationFactory extends Factory {
	private static final long serialVersionUID = 1L;

	public Component readAndProcess(ConfigAndMappingConfigBeans configAndMappingConfigBeans, LinkedHashMap<String, Component> components,
	        StatsType globalStatsType) throws Exception {
		try {
			countInputs(configAndMappingConfigBeans.getConfigBean(), components);
			Component component = baseProcess(configAndMappingConfigBeans.getConfigBean(), components, configAndMappingConfigBeans.getErrorHandlerSink());

			if (configAndMappingConfigBeans.getConfigBean().isPersist()) {
				component.persistAll();
			}

			Component finalComponent = postProcess(configAndMappingConfigBeans, component, globalStatsType);
			count(CounterType.OUTPUT, finalComponent);
			return finalComponent;
		} catch (Exception exception) {
			// TODO exception handling and error logging
			throw exception;
		}
	}

	private Component postProcess(ConfigAndMappingConfigBeans configAndMappingConfigBeans, Component component, StatsType globalStatsType)
	        throws ImproperValidationException, UnimplementedException, InvalidConfigException, UnsupportedException, InvalidRelationException, Exception {
		return mapping(configAndMappingConfigBeans, configAndMappingConfigBeans.getStatsStore(), component,
		        configAndMappingConfigBeans.getConfigBean().getSourceStream(), globalStatsType);
	}

	protected abstract Component baseProcess(ConfigBean configBean, LinkedHashMap<String, Component> components, ErrorHandlerSink errorHandlerSink)
	        throws Exception;

	protected abstract void countInputs(ConfigBean configBean, LinkedHashMap<String, Component> components) throws Exception;
}