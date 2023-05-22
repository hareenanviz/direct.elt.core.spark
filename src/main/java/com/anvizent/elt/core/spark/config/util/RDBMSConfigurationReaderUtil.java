package com.anvizent.elt.core.spark.config.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.LinkedHashMap;
import java.util.Properties;

import com.anvizent.elt.core.listener.common.connection.RDBMSConnection;
import com.anvizent.elt.core.spark.config.ConfigurationReadingException;
import com.anvizent.elt.core.spark.constant.ConfigConstants;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.Constants;
import com.anvizent.elt.core.spark.constant.Constants.ExceptionMessage;
import com.anvizent.elt.core.spark.constant.Constants.General.Operator;
import com.anvizent.elt.core.spark.exception.ConfigReferenceNotFoundException;
import com.anvizent.elt.core.spark.exception.InvalidInputForConfigException;
import com.anvizent.elt.core.spark.util.MutableInteger;
import com.anvizent.elt.core.spark.util.StringUtil;
import com.anvizent.encryptor.AnvizentEncryptor;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class RDBMSConfigurationReaderUtil {

	public static Connection getConnection(RDBMSConnection rdbmsConnection) throws ConfigurationReadingException {
		if (rdbmsConnection == null || rdbmsConnection.isNull()) {
			return null;
		} else {
			try {
				Class.forName(rdbmsConnection.getDriver());
				return DriverManager.getConnection(rdbmsConnection.getJdbcURL(), rdbmsConnection.getUserName(), rdbmsConnection.getPassword());
			} catch (ClassNotFoundException | SQLException exception) {
				throw new ConfigurationReadingException(exception.getMessage(), exception);
			}
		}
	}

	public static String decryptQuery(String query, AnvizentEncryptor anvizentEncryptor) throws ConfigurationReadingException {
		System.out.println("Query:" + query);

		if (query == null || query.isEmpty()) {
			return null;
		} else if (anvizentEncryptor == null) {
			return query;
		} else {
			int index = 0;
			while (true) {
				index = query.indexOf(ConfigConstants.Reading.RDBMS.ENCRYPTION_START, index);
				if (index == -1) {
					break;
				}
				index += 3;
				if (index >= query.length()) {
					break;
				}

				int endIndex = query.indexOf(ConfigConstants.Reading.RDBMS.ENCRYPTION_END, index);
				if (endIndex == -1) {
					break;
				}

				String toDecrypt = query.substring(index, endIndex);
				try {
					String decrypted = anvizentEncryptor.decrypt(toDecrypt);
					query = StringUtil.replaceAt(query, decrypted, index - 3, endIndex);
				} catch (Exception exception) {
					throw new ConfigurationReadingException(exception.getMessage(), exception);
				}

			}

			return query;
		}
	}

	public static Properties getValuesProperties(String valuesSourceQueryName, String valuesSourceQuery, AnvizentEncryptor anvizentEncryptor,
	        Connection connection) throws ConfigurationReadingException {
		Properties properties = new Properties();

		if (valuesSourceQuery == null || valuesSourceQuery.isEmpty()) {
			return properties;
		}

		valuesSourceQuery = decryptQuery(valuesSourceQuery, anvizentEncryptor);
		Statement statement = null;
		ResultSet resultSet = null;

		try {
			statement = connection.createStatement();
			resultSet = statement.executeQuery(valuesSourceQuery);
			int columnCount = resultSet.getMetaData().getColumnCount();

			if (columnCount == 2) {
				loadKeyValuePairs(valuesSourceQueryName, properties, resultSet);
			} else if (columnCount == 1) {
				loadText(valuesSourceQueryName, properties, resultSet);
			} else {
				throw new ConfigurationReadingException(
				        "Query `" + valuesSourceQuery + "` returning `" + columnCount + "` number of columns. Expected 1 or 2.");
			}

		} catch (SQLException exception) {
			throw new ConfigurationReadingException(exception.getMessage(), exception);
		} finally {
			close(resultSet, statement);
		}

		return properties;
	}

	public static void closeConnection(Connection connection) throws ConfigurationReadingException {
		try {
			if (connection != null && !connection.isClosed()) {
				connection.close();
			}
		} catch (SQLException exception) {
			throw new ConfigurationReadingException(exception.getMessage(), exception);
		}
	}

	private static void close(ResultSet resultSet, Statement statement) throws ConfigurationReadingException {
		try {
			if (resultSet != null && !resultSet.isClosed()) {
				resultSet.close();
			}

			if (statement != null && !statement.isClosed()) {
				statement.close();
			}
		} catch (SQLException exception) {
			throw new ConfigurationReadingException(exception.getMessage(), exception);
		}
	}

	private static void loadText(String valuesSourceQueryName, Properties properties, ResultSet resultSet) throws SQLException, ConfigurationReadingException {
		while (resultSet.next()) {
			setKeyValuesFromText(valuesSourceQueryName, properties, resultSet);
		}
	}

	private static void setKeyValuesFromText(String valuesSourceQueryName, Properties properties, ResultSet resultSet) throws SQLException {
		String linesString = resultSet.getString(1);

		if (linesString == null) {
			return;
		}

		String[] lines = linesString.split("\\r?\\n");

		for (int i = 0; i < lines.length; i++) {
			setKeyValuesFromText(properties, lines[i]);
		}
	}

	private static void setKeyValuesFromText(Properties properties, String line) throws SQLException {
		if (ConfigurationReaderUtil.isCommentOrEmptyLine(line)) {
			return;
		}

		int index = line.indexOf(Operator.EQUAL_TO);
		if (index != -1) {
			String key = line.substring(0, index);
			String value = line.substring(index + 1);

			properties.put(key, value);
		}
	}

	private static void loadKeyValuePairs(String valuesSourceQueryName, Properties properties, ResultSet resultSet)
	        throws SQLException, ConfigurationReadingException {
		while (resultSet.next()) {
			setKeyValuePairs(valuesSourceQueryName, properties, resultSet);
		}
	}

	private static void setKeyValuePairs(String valuesSourceQueryName, Properties properties, ResultSet resultSet)
	        throws SQLException, ConfigurationReadingException {
		String key = resultSet.getString(1);
		String value = resultSet.getString(2);

		if (key == null || key.isEmpty()) {
			throw new ConfigurationReadingException(
			        "Configuration key not found for query `" + valuesSourceQueryName + "` at record number `" + resultSet.getRow() + "`");
		}

		if (value == null) {
			value = "";
		}

		properties.put(key, value);
	}

	public static ResultSet getConfigsStatement(String configsSourceQuery, AnvizentEncryptor anvizentEncryptor, Statement configsStatement)
	        throws ConfigurationReadingException, SQLException {
		configsSourceQuery = RDBMSConfigurationReaderUtil.decryptQuery(configsSourceQuery, anvizentEncryptor);

		System.out.println("configsSourceQuery: " + configsSourceQuery);

		return configsStatement.executeQuery(configsSourceQuery);
	}

	public static LinkedHashMap<String, String> getConfig(String configName, ResultSet configsResultSet, MutableInteger recordNumber,
	        Properties valuesProperties, Properties globalValuesProperties, String eoc, String source)
	        throws InvalidInputForConfigException, SQLException, ConfigurationReadingException, ConfigReferenceNotFoundException {
		LinkedHashMap<String, String> config = new LinkedHashMap<>();

		while (configsResultSet.next()) {
			String key = getConfigKey(configsResultSet, recordNumber);

			if (key.startsWith("#")) {
				continue;
			}

			validateKeyWithConfigName(key, configName, recordNumber);

			if (key.endsWith(eoc)) {
				ConfigurationReaderUtil.validateStartAndEndOfMapping(config, configName);
				ConfigurationReaderUtil.validateStartAndEndOfStats(config, configName);
				ConfigurationReaderUtil.validateStartAndEndOfErrorHandler(config, configName);
				return config;
			}

			validateConfigContainsSpace(key, recordNumber);

			putConfig(config, configsResultSet, key, configName, recordNumber, valuesProperties, globalValuesProperties, source);
		}

		throw new InvalidInputForConfigException(MessageFormat.format(ExceptionMessage.END_OF_COMPONENT_NOT_FOUND, configName),
		        ExceptionMessage.AT_RECORD_NUMBER, recordNumber);
	}

	public static LinkedHashMap<String, String> getDerivedConfig(ResultSet configsResultSet, String configName, MutableInteger recordNumber)
	        throws SQLException, InvalidInputForConfigException {
		LinkedHashMap<String, String> configs = new LinkedHashMap<>();
		configs.put(configName, "");

		while (configsResultSet.next()) {
			String key = getConfigKey(configsResultSet, recordNumber);

			validateKeyWithConfigName(key, configName, recordNumber);
			validateMappingConfig(key, configName, recordNumber);

			if (key.endsWith(General.EOC)) {
				configs.put(key, "");
				return configs;
			}

			validateConfigContainsSpace(key, recordNumber);
			String value = configsResultSet.getString(2);

			if (value == null) {
				value = "";
			}

			configs.put(key, value);
		}

		throw new InvalidInputForConfigException(MessageFormat.format(ExceptionMessage.END_OF_COMPONENT_NOT_FOUND, configName),
		        ExceptionMessage.AT_RECORD_NUMBER, recordNumber);
	}

	public static String getConfigKey(ResultSet configsResultSet, MutableInteger recordNumber) throws SQLException {
		recordNumber.add(1);
		return configsResultSet.getString(1);
	}

	private static void validateMappingConfig(String line, String configName, MutableInteger recordNumber) throws InvalidInputForConfigException {
		if (line.contains(General.SOM) || line.contains(General.EOM)) {
			// TODO place in proper position
			// throw new
			// InvalidInputForConfigException(ExceptionMessage.DERIVED_COMPONENT_CANNOT_HAVE_MAPPING,
			// ExceptionMessage.AT_RECORD_NUMBER, recordNumber);
		}
	}

	private static void validateKeyWithConfigName(String key, String configName, MutableInteger recordNumber) throws InvalidInputForConfigException {
		if (key == null || !key.startsWith(configName)) {
			throw new InvalidInputForConfigException(MessageFormat.format(ExceptionMessage.CONFIG_IS_NOT_PROPERLY_ENDED, configName),
			        ExceptionMessage.AT_RECORD_NUMBER, recordNumber);
		}
	}

	public static void validateConfigContainsSpace(String key, MutableInteger recordNumber) throws InvalidInputForConfigException {
		if (key.contains(" ")) {
			throw new InvalidInputForConfigException(ExceptionMessage.CONFIGURATION_KEY_CANNOT_CONTAIN_SPACE, ExceptionMessage.AT_RECORD_NUMBER, recordNumber);
		}
	}

	private static void putConfig(LinkedHashMap<String, String> config, ResultSet configsResultSet, String key, String configName, MutableInteger recordNumber,
	        Properties valuesProperties, Properties globalValuesProperties, String source)
	        throws ConfigurationReadingException, SQLException, ConfigReferenceNotFoundException {
		String configValue = configsResultSet.getString(2);
		String configKey = key.replaceFirst(configName + Constants.General.KEY_SEPARATOR, "");

		if (configValue == null) {
			configValue = "";
		}

		config.put(configKey, ConfigurationReaderUtil.replacePlaceHolders(configKey, configValue, valuesProperties, globalValuesProperties, source));
	}

	public static String putConfig(LinkedHashMap<String, String> config, String configName, String currentLines, ResultSet configsResultSet,
	        MutableInteger recordNumber, Properties valuesProperties, Properties globalValuesProperties, String source)
	        throws SQLException, InvalidInputForConfigException, ConfigReferenceNotFoundException {
		if (currentLines == null || currentLines.length() == 0) {
			if (configsResultSet.next()) {
				recordNumber.add(1);
				currentLines = configsResultSet.getString(1);
			} else {
				throw new InvalidInputForConfigException(MessageFormat.format(ExceptionMessage.END_OF_COMPONENT_NOT_FOUND, configName),
				        ExceptionMessage.AT_RECORD_NUMBER, recordNumber);
			}
		}

		while (true) {
			String[] lines = StringUtil.splitByFirstOccurance(currentLines, "\r\n", "\n");
			String currentLine = lines[0];

			if (!ConfigurationReaderUtil.isCommentOrEmptyLine(currentLine)) {
				if (currentLine == null || !currentLine.startsWith(configName)) {
					throw new InvalidInputForConfigException(MessageFormat.format(ExceptionMessage.CONFIG_IS_NOT_PROPERLY_ENDED, configName),
					        ExceptionMessage.AT_RECORD_NUMBER, recordNumber);
				}

				if (currentLine.endsWith(General.EOC)) {
					return getCurrentLines(lines, configsResultSet, recordNumber, configName, true);
				}

				if (currentLine.trim().length() != 0) {
					putConfig(configName, currentLine, recordNumber, config, valuesProperties, globalValuesProperties, source);
				}
			}

			currentLines = getCurrentLines(lines, configsResultSet, recordNumber, configName, false);

		}
	}

	private static String getCurrentLines(String[] lines, ResultSet configsResultSet, MutableInteger recordNumber, String configName, boolean eocFound)
	        throws InvalidInputForConfigException, SQLException {
		if (lines.length == 1 || lines[1].length() == 0) {
			if (configsResultSet.next()) {
				recordNumber.add(1);
				return configsResultSet.getString(1);
			} else {
				if (eocFound) {
					return null;
				} else {
					throw new InvalidInputForConfigException(MessageFormat.format(ExceptionMessage.END_OF_COMPONENT_NOT_FOUND, configName),
					        ExceptionMessage.AT_RECORD_NUMBER, recordNumber);
				}
			}
		} else {
			return lines[1];
		}
	}

	private static void putConfig(String configName, String currentLine, MutableInteger recordNumber, LinkedHashMap<String, String> config,
	        Properties valuesProperties, Properties globalValuesProperties, String source)
	        throws InvalidInputForConfigException, ConfigReferenceNotFoundException {
		currentLine = currentLine.replaceFirst(configName + Constants.General.KEY_SEPARATOR, "");

		int index = currentLine.indexOf(Operator.EQUAL_TO);
		if (index == -1) {
			config.put(currentLine, null);
		} else {
			String key = currentLine.substring(0, index);
			if (key.contains(" ")) {
				throw new InvalidInputForConfigException(ExceptionMessage.CONFIGURATION_KEY_CANNOT_CONTAIN_SPACE, ExceptionMessage.AT_RECORD_NUMBER,
				        recordNumber);
			}
			String value = currentLine.substring(index + 1);

			config.put(key, ConfigurationReaderUtil.replacePlaceHolders(key, value, valuesProperties, globalValuesProperties, source));
		}
	}

	public static String getComponentName(ResultSet configsResultSet, MutableInteger recordNumber)
	        throws InvalidInputForConfigException, ConfigurationReadingException {
		try {
			while (configsResultSet.next()) {
				String configName = getConfigKey(configsResultSet, recordNumber);

				if (!configName.equals(Constants.General.DERIVED_COMPONENT)) {
					throw new InvalidInputForConfigException(ExceptionMessage.NOT_A_DERIVED_COMPONENT);
				}

				validateConfigContainsSpace(configName, recordNumber);

				return getComponentName(configsResultSet, configName, recordNumber);
			}
		} catch (SQLException exception) {
			throw new ConfigurationReadingException(exception.getMessage(), exception);
		}

		return null;
	}

	private static String getComponentName(ResultSet configsResultSet, String configName, MutableInteger recordNumber)
	        throws SQLException, InvalidInputForConfigException {
		String componentName = null;

		while (configsResultSet.next()) {
			String key = getConfigKey(configsResultSet, recordNumber);

			validateKeyWithConfigName(key, configName, recordNumber);

			if (key.endsWith(General.EOC)) {
				return componentName;
			}

			validateConfigContainsSpace(key, recordNumber);

			componentName = configsResultSet.getString(2);
			if (componentName == null || componentName.isEmpty()) {
				throw new InvalidInputForConfigException(ExceptionMessage.DERIVED_COMPONENT_NAME_CANNOT_BE_NULL_OR_EMPTY, ExceptionMessage.AT_RECORD_NUMBER,
				        recordNumber);
			} else if (componentName.contains(" ")) {
				throw new InvalidInputForConfigException(ExceptionMessage.DERIVED_COMPONENT_NAME_CANNOT_CONTAIN_SPACE, ExceptionMessage.AT_RECORD_NUMBER,
				        recordNumber);
			}
		}

		throw new InvalidInputForConfigException(MessageFormat.format(ExceptionMessage.END_OF_COMPONENT_NOT_FOUND, configName),
		        ExceptionMessage.AT_RECORD_NUMBER, recordNumber);
	}

}