package com.anvizent.elt.core.spark.config.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.maven.shared.filtering.PropertyUtils;

import com.anvizent.elt.core.spark.config.ConfigurationReadingException;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.Constants;
import com.anvizent.elt.core.spark.constant.Constants.ExceptionMessage;
import com.anvizent.elt.core.spark.constant.Constants.General.Operator;
import com.anvizent.elt.core.spark.exception.ConfigReferenceNotFoundException;
import com.anvizent.elt.core.spark.exception.InvalidInputForConfigException;
import com.anvizent.elt.core.spark.util.MutableInteger;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class IOConfigurationReaderUtil {

	public static LinkedHashMap<String, String> getConfig(String configName, BufferedReader configsReader, final MutableInteger lineNumber, Properties values,
	        Properties globalValues, String eoc, String source) throws IOException, InvalidInputForConfigException, ConfigReferenceNotFoundException {
		LinkedHashMap<String, String> config = new LinkedHashMap<String, String>();
		try {
			while (true) {
				String line = configsReader.readLine();

				validateEmptyLine(line, configName, lineNumber);

				lineNumber.add(1);

				if (ConfigurationReaderUtil.isCommentOrEmptyLine(line)) {
					continue;
				}

				validateLineWithConfigName(configName, line, lineNumber);

				if (line.endsWith(eoc)) {
					ConfigurationReaderUtil.validateStartAndEndOfMapping(config, configName);
					ConfigurationReaderUtil.validateStartAndEndOfStats(config, configName);
					ConfigurationReaderUtil.validateStartAndEndOfErrorHandler(config, configName);
					break;
				}

				if (line.trim().length() != 0) {
					line = line.replaceFirst(configName + Constants.General.KEY_SEPARATOR, "");
					putConfig(configName, line, lineNumber, config, values, globalValues, true, source);
				}
			}
		} catch (NullPointerException nullPointerException) {
			throw new InvalidInputForConfigException(MessageFormat.format(ExceptionMessage.END_OF_COMPONENT_NOT_FOUND, configName),
			        ExceptionMessage.AT_LINE_NUMBER, lineNumber);
		}

		return config;
	}

	private static void putConfig(String configName, String line, final MutableInteger lineNumber, LinkedHashMap<String, String> config, Properties values,
	        Properties globalValues, boolean isreplacePlaceHolders, String source) throws InvalidInputForConfigException, ConfigReferenceNotFoundException {
		int index = line.indexOf(Operator.EQUAL_TO);

		if (index == -1) {
			config.put(line, null);
		} else {
			String key = line.substring(0, index);

			validateConfigContainsSpace(key, lineNumber);

			String value = line.substring(index + 1);

			config.put(key, isreplacePlaceHolders ? ConfigurationReaderUtil.replacePlaceHolders(key, value, values, globalValues, source) : value);
		}
	}

	public static LinkedHashMap<String, String> getDerivedConfig(BufferedReader configsReader, String configName, MutableInteger lineNumber, String source)
	        throws IOException, InvalidInputForConfigException, ConfigReferenceNotFoundException {
		LinkedHashMap<String, String> configs = new LinkedHashMap<>();
		configs.put(configName, "");

		while (true) {
			String line = configsReader.readLine();

			validateEmptyLine(line, configName, lineNumber);
			validateMappingConfig(line, configName, lineNumber);

			lineNumber.add(1);

			if (ConfigurationReaderUtil.isCommentOrEmptyLine(line)) {
				continue;
			}

			validateLineWithConfigName(configName, line, lineNumber);

			if (line.endsWith(General.EOC)) {
				configs.put(line, "");
				break;
			}

			if (line.trim().length() != 0) {
				putConfig(configName, line, lineNumber, configs, null, null, false, source);
			}
		}

		return configs;
	}

	private static void validateMappingConfig(String line, String configName, MutableInteger lineNumber) throws InvalidInputForConfigException {
		if (line.contains(General.SOM) || line.contains(General.EOM)) {
			// TODO please it in proper place
			// throw new
			// InvalidInputForConfigException(ExceptionMessage.DERIVED_COMPONENT_CANNOT_HAVE_MAPPING,
			// ExceptionMessage.AT_LINE_NUMBER, lineNumber);
		}
	}

	private static void validateLineWithConfigName(String configName, String line, MutableInteger lineNumber) throws InvalidInputForConfigException {
		if (line == null || !line.startsWith(configName)) {
			throw new InvalidInputForConfigException(MessageFormat.format(ExceptionMessage.CONFIG_IS_NOT_PROPERLY_ENDED, configName),
			        ExceptionMessage.AT_LINE_NUMBER, lineNumber);
		}
	}

	private static void validateEmptyLine(String line, String configName, MutableInteger lineNumber) throws InvalidInputForConfigException {
		if (line == null) {
			throw new InvalidInputForConfigException(MessageFormat.format(ExceptionMessage.END_OF_COMPONENT_NOT_FOUND, configName),
			        ExceptionMessage.AT_LINE_NUMBER, lineNumber);
		}
	}

	public static void validateConfigContainsSpace(String key, MutableInteger lineNumber) throws InvalidInputForConfigException {
		if (key.contains(" ")) {
			throw new InvalidInputForConfigException(ExceptionMessage.CONFIGURATION_KEY_CANNOT_CONTAIN_SPACE, ExceptionMessage.AT_LINE_NUMBER, lineNumber);
		}
	}

	public static BufferedReader getConfigReader(String fileName) throws FileNotFoundException {
		return new BufferedReader(new FileReader(new File(fileName)));
	}

	public static Properties getValuesProperties(String valuesPropertiesFilePath) throws ConfigurationReadingException {
		Properties properties = new Properties();

		if (StringUtils.isNotEmpty(valuesPropertiesFilePath)) {
			FileInputStream inputStream;
			try {
				inputStream = new FileInputStream(valuesPropertiesFilePath);
				properties.load(inputStream);
				properties = PropertyUtils.loadPropertyFile(new File(valuesPropertiesFilePath), properties);
				inputStream.close();
			} catch (IOException ioException) {
				throw new ConfigurationReadingException(ioException.getMessage(), ioException);
			}
		}

		return properties;
	}

	public static String getComponentName(BufferedReader configsReader, String currentLine, MutableInteger lineNumber)
	        throws InvalidInputForConfigException, ConfigurationReadingException {
		String componentName = null;

		while (currentLine != null) {
			lineNumber.add(1);

			if (!ConfigurationReaderUtil.isCommentOrEmptyLine(currentLine)) {
				if (!currentLine.startsWith(Constants.General.DERIVED_COMPONENT)) {
					throw new InvalidInputForConfigException(ExceptionMessage.NOT_A_DERIVED_COMPONENT);
				}

				validateConfigContainsSpace(currentLine, lineNumber);

				if (currentLine.contains(Constants.General.PROPERTY_COMMENT)) {
					currentLine = currentLine.substring(0, currentLine.indexOf(Constants.General.PROPERTY_COMMENT));
				}

				try {
					return getComponentName(currentLine, configsReader, lineNumber);
				} catch (IOException exception) {
					throw new ConfigurationReadingException(exception.getMessage(), exception);
				}
			}
		}

		return componentName;
	}

	private static String getComponentName(String configName, BufferedReader configsReader, MutableInteger lineNumber)
	        throws IOException, InvalidInputForConfigException {
		String componentName = null;

		while (true) {
			String line = configsReader.readLine();

			validateEmptyLine(line, configName, lineNumber);

			lineNumber.add(1);

			if (ConfigurationReaderUtil.isCommentOrEmptyLine(line)) {
				continue;
			}

			validateLineWithConfigName(configName, line, lineNumber);

			if (line.endsWith(General.EOC)) {
				break;
			}

			if (line.trim().length() != 0) {
				componentName = getComponentName(line);
				if (componentName == null || componentName.isEmpty()) {
					throw new InvalidInputForConfigException(ExceptionMessage.DERIVED_COMPONENT_NAME_CANNOT_BE_NULL_OR_EMPTY, ExceptionMessage.AT_LINE_NUMBER,
					        lineNumber);
				} else if (componentName.contains(" ")) {
					throw new InvalidInputForConfigException(ExceptionMessage.DERIVED_COMPONENT_NAME_CANNOT_CONTAIN_SPACE, ExceptionMessage.AT_LINE_NUMBER,
					        lineNumber);
				} else if (!Pattern.matches(Constants.General.DERIVED_COMPONENT_NAME_REGEX, componentName)) {
					throw new InvalidInputForConfigException(ExceptionMessage.DERIVED_COMPONENT_NAME_CANNOT_CONTAIN_ANY_SPECIAL_CHARACTER,
					        ExceptionMessage.AT_LINE_NUMBER, lineNumber);
				}
			}
		}

		return componentName;
	}

	private static String getComponentName(String line) {
		int index = line.indexOf(Operator.EQUAL_TO);
		if (index != -1) {
			return line.substring(index + 1).trim();
		} else {
			return null;
		}
	}

}