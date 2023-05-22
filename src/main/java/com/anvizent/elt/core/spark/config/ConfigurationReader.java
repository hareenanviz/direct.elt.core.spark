package com.anvizent.elt.core.spark.config;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Properties;

import com.anvizent.elt.core.lib.config.bean.SeekDetails;
import com.anvizent.elt.core.spark.exception.ConfigReferenceNotFoundException;
import com.anvizent.elt.core.spark.exception.DerivedComponentsLoopException;
import com.anvizent.elt.core.spark.exception.InvalidInputForConfigException;
import com.anvizent.elt.core.spark.util.LIFOQueue;
import com.anvizent.elt.core.spark.util.MutableInteger;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public abstract class ConfigurationReader implements Serializable {
	private static final long serialVersionUID = 1L;

	protected final String source;
	protected final Properties valuesProperties;
	protected final Properties globalValuesProperties;
	protected final MutableInteger seek = new MutableInteger();
	protected final LinkedHashMap<String, DerivedComponentConfiguration> derivedComponents;
	protected LIFOQueue<MapConfigurationReader> mapConfigurationReaders = new LIFOQueue<>();

	public MutableInteger getSeek() {
		return seek;
	}

	public ConfigurationReader(String source, Properties valuesProperties, Properties globalValuesProperties,
			LinkedHashMap<String, DerivedComponentConfiguration> derivedComponents) {
		this.source = source;
		this.valuesProperties = valuesProperties;
		this.globalValuesProperties = globalValuesProperties;
		this.derivedComponents = derivedComponents;
	}

	public ComponentConfiguration getNextConfiguration()
			throws ConfigurationReadingException, InvalidInputForConfigException, ConfigReferenceNotFoundException {
		MapConfigurationReader mapConfigurationReader = mapConfigurationReaders.peek();
		if (mapConfigurationReader != null) {
			ComponentConfiguration componentConfiguration = mapConfigurationReader.getNextConfiguration();

			if (componentConfiguration == null) {
				mapConfigurationReaders.poll();
				return getNextConfiguration();
			} else {
				return getComponentConfiguration(componentConfiguration.getConfiguration(), componentConfiguration.getConfigurationName(),
						componentConfiguration.getSeekDetails().getFrom(), componentConfiguration.getSeekDetails().getTo(),
						componentConfiguration.getSeekDetails().getName(), componentConfiguration.getSeekDetails().getSourceName(), false);
			}
		} else {
			return getComponentConfiguration();
		}
	}

	protected ComponentConfiguration getComponentConfiguration(LinkedHashMap<String, String> config, String configName, int initialSeek, int finalSeek,
			String seekName, String source, boolean computeListener) throws ConfigReferenceNotFoundException, ConfigurationReadingException {
		if (derivedComponents != null && !derivedComponents.isEmpty() && derivedComponents.containsKey(configName)) {
			MapConfigurationReader mapConfigurationReader = new MapConfigurationReader(derivedComponents.get(configName), config);

			if (computeListener) {
				afterComponentConfigurationCreated();
			}

			int index = mapConfigurationReaders.indexOf(mapConfigurationReader);

			if (index != -1) {
				throw new ConfigurationReadingException(new DerivedComponentsLoopException(mapConfigurationReaders, index));
			}

			mapConfigurationReaders.add(mapConfigurationReader);
			ComponentConfiguration componentConfiguration = mapConfigurationReader.getNextConfiguration();

			return getComponentConfiguration(componentConfiguration.getConfiguration(), componentConfiguration.getConfigurationName(),
					componentConfiguration.getSeekDetails().getFrom(), componentConfiguration.getSeekDetails().getTo(),
					componentConfiguration.getSeekDetails().getName(), componentConfiguration.getSeekDetails().getSourceName(), false);
		} else {
			ComponentConfiguration componentConfiguration = new ComponentConfiguration(configName, new SeekDetails(initialSeek, finalSeek, seekName, source),
					config);

			if (computeListener) {
				afterComponentConfigurationCreated();
			}

			return componentConfiguration;
		}
	}

	protected abstract ComponentConfiguration getComponentConfiguration()
			throws ConfigurationReadingException, ConfigReferenceNotFoundException, InvalidInputForConfigException;

	protected abstract void afterComponentConfigurationCreated() throws ConfigurationReadingException, ConfigReferenceNotFoundException;

	public abstract String getSeekName();

}
