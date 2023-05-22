package com.anvizent.elt.core.spark.source.factory;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.constant.Constants.StatsNames;
import com.anvizent.elt.core.lib.constant.StatsCategory;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.lib.stats.calculator.AnvizentFromRowStatsCalculator;
import com.anvizent.elt.core.lib.stats.calculator.IStatsCalculator;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.listener.common.constant.StatsType;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.constant.CounterType;
import com.anvizent.elt.core.spark.factory.Factory;

/**
 * @author Hareen Bejjanki
 *
 */
public abstract class SourceFactory extends Factory {

	private static final long serialVersionUID = 1L;

	public Component readAndProcess(ConfigAndMappingConfigBeans configAndMappingConfigBeans, StatsType globalStatsType) throws Exception {
		try {
			Component component = createSourceComponent(configAndMappingConfigBeans);

			count(CounterType.READ, component);
			Component finalComponent = postProcess(configAndMappingConfigBeans, component, globalStatsType);

			return finalComponent;
		} catch (Exception exception) {
			// TODO exception handling
			throw exception;
		}
	}

	private Component postProcess(ConfigAndMappingConfigBeans configAndMappingConfigBeans, Component component, StatsType globalStatsType)
	        throws ImproperValidationException, UnimplementedException, InvalidConfigException, UnsupportedException, InvalidRelationException, Exception {
		return mapping(configAndMappingConfigBeans, configAndMappingConfigBeans.getStatsStore(), component,
		        configAndMappingConfigBeans.getConfigBean().getSourceStream(), globalStatsType);
	}

	@Override
	protected IStatsCalculator getStatsCalculator(StatsCategory statsCategory, String statsName, String internalRDDName) {
		return new AnvizentFromRowStatsCalculator(statsCategory, statsName);
	}

	@Override
	public void createFactoryAccumulators(ConfigBean configBean, StatsType statsType, boolean componentLevel) {
		AnvizentAccumulator readAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(), getName(),
		        StatsCategory.IN, StatsNames.READ, true, new AnvizentFromRowStatsCalculator(StatsCategory.IN, StatsNames.READ));

		ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName(), readAnvizentAccumulator);
	}

	protected abstract Component createSourceComponent(ConfigAndMappingConfigBeans configAndMappingConfigBeans)
	        throws ImproperValidationException, UnsupportedException, Exception;
}