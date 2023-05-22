package com.anvizent.elt.core.spark.config.util;

import java.text.MessageFormat;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.Properties;

import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.Constants;
import com.anvizent.elt.core.spark.constant.Constants.ExceptionMessage;
import com.anvizent.elt.core.spark.exception.ConfigReferenceNotFoundException;
import com.anvizent.elt.core.spark.exception.InvalidInputForConfigException;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ConfigurationReaderUtil {

	public static String replacePlaceHolders(String config, String configValue, Properties valuesProperties, Properties globalValuesProperties, String source)
	        throws ConfigReferenceNotFoundException {
		configValue = setValuesProperties(valuesProperties, General.CONFIG_START_PLACEHOLDER, General.CONFIG_END_PLACEHOLDER, configValue);
		configValue = setValuesProperties(globalValuesProperties, General.CONFIG_START_PLACEHOLDER, General.CONFIG_END_PLACEHOLDER, configValue);

		configValue = replaceInnerValues(config, configValue, valuesProperties);
		configValue = replaceInnerValues(config, configValue, globalValuesProperties);

		if (configValue.contains(General.CONFIG_START_PLACEHOLDER)) {
			throw new ConfigReferenceNotFoundException(MessageFormat.format(ExceptionMessage.REFERENCE_NOT_FOUND_FOR_CONFIG, configValue, config, source));
		}

		return configValue;
	}

	private static String replaceInnerValues(String configKey, String configValue, Properties valuesProperties) {
		if (configValue.contains(General.CONFIG_START_PLACEHOLDER)) {
			String innerKey = configValue.substring(configValue.indexOf(General.CONFIG_START_PLACEHOLDER) + 2,
			        configValue.indexOf(General.CONFIG_END_PLACEHOLDER));
			String innerValue = (String) valuesProperties.get(innerKey);

			if (innerValue != null && !innerValue.isEmpty()) {
				return replaceInnerValues(configKey,
				        configValue.replace(General.CONFIG_START_PLACEHOLDER + innerKey + General.CONFIG_END_PLACEHOLDER, innerValue), valuesProperties);
			} else {
				return configValue;
			}
		} else {
			return configValue;
		}
	}

	public static boolean isCommentOrEmptyLine(String line) {
		line = line.trim();
		return line.length() == 0 || line.startsWith(Constants.General.PROPERTY_COMMENT);
	}

	private static String setValuesProperties(Properties properties, String prefix, String suffix, String config) {
		for (Entry<Object, Object> key : properties.entrySet()) {
			config = config.replace(prefix + key.getKey() + suffix, (String) key.getValue());
		}

		return config;
	}

	public static String replaceConfig(String key, String replaceFrom, String replaceTo) {
		if (key == null) {
			return null;
		}

		return key.replace(replaceFrom, replaceTo);
	}

	public static void validateStartAndEndOfStats(LinkedHashMap<String, String> config, String configName) throws InvalidInputForConfigException {
		if (config.containsKey(General.SOS) && !config.containsKey(General.EOS)) {
			throw new InvalidInputForConfigException(MessageFormat.format(ExceptionMessage.END_OF_STATS_NOT_FOUND, configName));
		} else if (!config.containsKey(General.SOS) && config.containsKey(General.EOS)) {
			throw new InvalidInputForConfigException(MessageFormat.format(ExceptionMessage.START_OF_STATS_NOT_FOUND, configName));
		}
	}

	public static void validateStartAndEndOfMapping(LinkedHashMap<String, String> config, String configName) throws InvalidInputForConfigException {
		if (config.containsKey(General.SOM) && !config.containsKey(General.EOM)) {
			throw new InvalidInputForConfigException(MessageFormat.format(ExceptionMessage.END_OF_MAPPING_NOT_FOUND, configName));
		} else if (!config.containsKey(General.SOM) && config.containsKey(General.EOM)) {
			throw new InvalidInputForConfigException(MessageFormat.format(ExceptionMessage.START_OF_MAPPING_NOT_FOUND, configName));
		}
	}

	public static void validateStartAndEndOfErrorHandler(LinkedHashMap<String, String> config, String configName) throws InvalidInputForConfigException {
		if (config.containsKey(General.SOEH) && !config.containsKey(General.EOEH)) {
			throw new InvalidInputForConfigException(MessageFormat.format(ExceptionMessage.END_OF_EH_NOT_FOUND, configName));
		} else if (!config.containsKey(General.SOEH) && config.containsKey(General.EOEH)) {
			throw new InvalidInputForConfigException(MessageFormat.format(ExceptionMessage.START_OF_EH_NOT_FOUND, configName));
		}
	}
}
