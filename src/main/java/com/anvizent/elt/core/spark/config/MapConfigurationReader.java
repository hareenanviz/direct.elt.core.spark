package com.anvizent.elt.core.spark.config;

import java.text.MessageFormat;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

import com.anvizent.elt.core.lib.config.bean.SeekDetails;
import com.anvizent.elt.core.spark.config.util.ConfigurationReaderUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.Constants;
import com.anvizent.elt.core.spark.constant.Constants.ExceptionMessage;
import com.anvizent.elt.core.spark.exception.ConfigReferenceNotFoundException;
import com.anvizent.elt.core.spark.exception.InvalidInputForConfigException;
import com.anvizent.elt.core.spark.util.MutableInteger;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class MapConfigurationReader extends ConfigurationReader {

	private static final long serialVersionUID = 1L;

	private final String name;
	private DerivedComponentConfiguration derivedComponentConfiguration;
	private MutableInteger derivedComponentIndex = new MutableInteger();

	public String getName() {
		return name;
	}

	public MapConfigurationReader(DerivedComponentConfiguration derivedComponentConfiguration, LinkedHashMap<String, String> config) {
		super(derivedComponentConfiguration.getSeekDetails().getSourceName(), getProperties(config), new Properties(), new LinkedHashMap<>());
		this.name = derivedComponentConfiguration.getConfigurationName();
		this.derivedComponentConfiguration = derivedComponentConfiguration;
	}

	private static Properties getProperties(LinkedHashMap<String, String> config) {
		Properties properties = new Properties();
		properties.putAll(config);
		return properties;
	}

	@Override
	public ComponentConfiguration getNextConfiguration() throws ConfigReferenceNotFoundException {
		if (derivedComponentIndex.get() == derivedComponentConfiguration.getConfigurations().size()) {
			return null;
		}

		ComponentConfiguration currentComponentConfiguration = derivedComponentConfiguration.getConfigurations().get(derivedComponentIndex.get());

		LinkedHashMap<String, String> derivedConfig = replaceDerivedConfigurationValues(valuesProperties, currentComponentConfiguration.getConfiguration(),
		        currentComponentConfiguration.getConfigurationName());

		derivedComponentIndex.add(1);

		return new ComponentConfiguration(currentComponentConfiguration.getConfigurationName(),
		        new SeekDetails(currentComponentConfiguration.getSeekDetails().getFrom(), currentComponentConfiguration.getSeekDetails().getTo(),
		                currentComponentConfiguration.getSeekDetails().getName(), currentComponentConfiguration.getSeekDetails().getSourceName()),
		        derivedConfig);
	}

	private LinkedHashMap<String, String> replaceDerivedConfigurationValues(Properties valuesProperties, LinkedHashMap<String, String> derivedConfiguration,
	        String configName) throws ConfigReferenceNotFoundException {
		LinkedHashMap<String, String> newDerivedConfiguration = new LinkedHashMap<>();

		for (Entry<String, String> entry : derivedConfiguration.entrySet()) {
			String derivedKey = entry.getKey();
			String derivedValue = entry.getValue();

			putConfig(newDerivedConfiguration, configName, derivedKey, getDerivedValue(derivedKey, derivedValue));
		}

		removeMetaConfigs(newDerivedConfiguration, configName);

		return newDerivedConfiguration;
	}

	private String getDerivedValue(String derivedKey, String derivedValue) throws ConfigReferenceNotFoundException {
		while (StringUtils.contains(derivedValue, General.CONFIG_START_PLACEHOLDER)) {
			String configKey = StringUtils.substringBetween(derivedValue, General.CONFIG_START_PLACEHOLDER, General.CONFIG_END_PLACEHOLDER);
			derivedValue = getDerivedValue(configKey, derivedKey, derivedValue);
		}

		return derivedValue;
	}

	private String getDerivedValue(String configKey, String derivedKey, String derivedValue) throws ConfigReferenceNotFoundException {
		if (configKey != null && valuesProperties.containsKey(configKey)) {
			return ConfigurationReaderUtil.replaceConfig(derivedValue, General.CONFIG_START_PLACEHOLDER + configKey + General.CONFIG_END_PLACEHOLDER,
			        valuesProperties.getProperty(configKey));
		} else if (configKey != null && !valuesProperties.containsKey(configKey)) {
			return getMissingDerivedValue(configKey, derivedKey, derivedValue, derivedComponentConfiguration.getSeekDetails().getSourceName());
		} else {
			return "";
		}
	}

	private static String getMissingDerivedValue(String configKey, String derivedKey, String derivedValue, String source)
	        throws ConfigReferenceNotFoundException {
		if (derivedValue.equals(General.CONFIG_START_PLACEHOLDER + configKey + General.CONFIG_END_PLACEHOLDER)) {
			return "";
		} else {
			throw new ConfigReferenceNotFoundException(MessageFormat.format(ExceptionMessage.REFERENCE_NOT_FOUND_FOR_CONFIG, derivedKey, configKey, source));
		}
	}

	private void putConfig(LinkedHashMap<String, String> newDerivedConfiguration, String configName, String derivedKey, String derivedValue)
	        throws ConfigReferenceNotFoundException {
		newDerivedConfiguration.put(ConfigurationReaderUtil.replaceConfig(derivedKey, configName + Constants.General.KEY_SEPARATOR, ""), derivedValue);
	}

	private static void removeMetaConfigs(LinkedHashMap<String, String> newDerivedConfiguration, String configName) {
		newDerivedConfiguration.remove(configName);
		newDerivedConfiguration.remove(ConfigConstants.General.EOC);
	}

	@Override
	public String getSeekName() {
		return ExceptionMessage.AT_COMPONENT_NUMBER;
	}

	@Override
	protected ComponentConfiguration getComponentConfiguration()
	        throws ConfigurationReadingException, ConfigReferenceNotFoundException, InvalidInputForConfigException {
		return null;
	}

	@Override
	protected void afterComponentConfigurationCreated() throws ConfigurationReadingException, ConfigReferenceNotFoundException {
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
		        + ((derivedComponentConfiguration.getConfigurationName() == null) ? 0 : derivedComponentConfiguration.getConfigurationName().hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		} else if (obj == null) {
			return false;
		} else if (getClass() != obj.getClass()) {
			return false;
		} else {
			MapConfigurationReader other = (MapConfigurationReader) obj;

			if (derivedComponentConfiguration.getConfigurationName() == null) {
				if (other.derivedComponentConfiguration.getConfigurationName() != null) {
					return false;
				}
			} else if (!derivedComponentConfiguration.getConfigurationName().equals(other.derivedComponentConfiguration.getConfigurationName())) {
				return false;
			}

			return true;
		}
	}

}
