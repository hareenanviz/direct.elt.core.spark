package com.anvizent.elt.core.spark.reader;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.SeekDetails;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.listener.common.constant.StatsType;
import com.anvizent.elt.core.listener.common.store.StatsSettingsStore;
import com.anvizent.elt.core.spark.config.ComponentConfiguration;
import com.anvizent.elt.core.spark.config.ConfigurationReader;
import com.anvizent.elt.core.spark.config.ConfigurationReaderBuilder;
import com.anvizent.elt.core.spark.config.ConfigurationReadingException;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General.StatsSettings;
import com.anvizent.elt.core.spark.constant.ConfigConstants.SQLNoSQL;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant;
import com.anvizent.elt.core.spark.constant.Constants.ARGSConstant;
import com.anvizent.elt.core.spark.exception.ConfigReferenceNotFoundException;
import com.anvizent.elt.core.spark.exception.InvalidConfigurationReaderProperty;
import com.anvizent.elt.core.spark.exception.InvalidInputForConfigException;
import com.anvizent.elt.core.spark.exception.StatsSettingsException;

/**
 * @author Hareen Bejjanki
 *
 */
public class StatsSettingsReader {

	public static StatsSettingsStore getStatsSettingsStore(LinkedHashMap<String, ArrayList<String>> arguments)
	        throws ConfigurationReadingException, InvalidInputForConfigException, ConfigReferenceNotFoundException, UnsupportedEncodingException,
	        InvalidConfigurationReaderProperty, StatsSettingsException, ImproperValidationException {

		if (!arguments.containsKey(ARGSConstant.STATS_SETTINGS)) {
			return null;
		}

		ConfigurationReader statsSettingsConfigurationReader = new ConfigurationReaderBuilder(arguments, null,
		        arguments.get(ARGSConstant.STATS_SETTINGS).get(0), ConfigConstants.General.EOS).build();
		StatsSettingsStore statsSettingsStore = createStatsSettingsStore(statsSettingsConfigurationReader.getNextConfiguration());

		return statsSettingsStore;
	}

	public static StatsSettingsStore createStatsSettingsStore(ComponentConfiguration componentConfiguration)
	        throws StatsSettingsException, ImproperValidationException {
		LinkedHashMap<String, String> statsSettingsConfigurations = componentConfiguration.getConfiguration();

		StatsSettingsException exception = createStatsSettingsException(componentConfiguration.getSeekDetails());

		String table = ConfigUtil.getString(statsSettingsConfigurations, SQLNoSQL.TABLE);
		String endPoint = ConfigUtil.getString(statsSettingsConfigurations, StatsSettings.END_POINT);
		ArrayList<String> constantNames = ConfigUtil.getArrayList(statsSettingsConfigurations, StatsSettings.CONSTANT_NAMES, exception);
		ArrayList<String> constantValues = ConfigUtil.getArrayList(statsSettingsConfigurations, StatsSettings.CONSTANT_VALUES, exception);
		ArrayList<Class<?>> constantTypes = ConfigUtil.getArrayListOfClass(statsSettingsConfigurations, StatsSettings.CONSTANT_TYPES, exception);
		Integer retryCount = ConfigUtil.getInteger(statsSettingsConfigurations, General.MAX_RETRY_COUNT, exception, General.DEFAULT_MAX_RETRY_COUNT);
		Long retryDelay = ConfigUtil.getLong(statsSettingsConfigurations, General.RETRY_DELAY, exception, General.DEFAULT_RETRY_DELAY);
		StatsType type = StatsType.getInstance(ConfigUtil.getString(statsSettingsConfigurations, StatsSettings.STATS_TYPE));

		ArrayList<String> host = ConfigUtil.getArrayList(statsSettingsConfigurations, SQLNoSQL.HOST, exception);
		String userName = ConfigUtil.getString(statsSettingsConfigurations, SQLNoSQL.USER_NAME);
		String password = ConfigUtil.getString(statsSettingsConfigurations, SQLNoSQL.PASSWORD);
		String dbName = ConfigUtil.getString(statsSettingsConfigurations, SQLNoSQL.DB_NAME);
		ArrayList<Integer> portNumber = ConfigUtil.getArrayListOfIntegers(statsSettingsConfigurations, SQLNoSQL.PORT_NUMBER, exception);
		Long timeout = ConfigUtil.getLong(statsSettingsConfigurations, SQLNoSQL.TIMEOUT, exception);

		StatsSettingsStore statsSettingsStore = new StatsSettingsStore(table, endPoint, constantNames, constantValues, constantTypes, type, retryCount,
		        retryDelay);

		statsSettingsStore.setRethinkDBConnection(host, portNumber, dbName, userName, password, timeout);

		validateStatsSettings(statsSettingsStore, exception);

		return statsSettingsStore;
	}

	private static StatsSettingsException createStatsSettingsException(SeekDetails seekDetails) {
		StatsSettingsException exception = new StatsSettingsException();

		exception.setComponent(StatsSettings.STATS_SETTINGS);
		exception.setComponentName(StatsSettings.STATS_SETTINGS_CONFIG_NAME);
		exception.setSeekDetails(seekDetails);

		return exception;
	}

	private static void validateStatsSettings(StatsSettingsStore statsSettingsStore, StatsSettingsException exception) throws StatsSettingsException {
		if (statsSettingsStore.getRethinkDBConnection().getHost() == null || statsSettingsStore.getRethinkDBConnection().getHost().isEmpty()) {
			exception.add(ValidationConstant.Message.SINGLE_KEY_MANDATORY, SQLNoSQL.HOST);
		}

		if (statsSettingsStore.getRethinkDBConnection().getDBName() == null || statsSettingsStore.getRethinkDBConnection().getDBName().isEmpty()) {
			exception.add(ValidationConstant.Message.SINGLE_KEY_MANDATORY, SQLNoSQL.DB_NAME);
		}

		if (statsSettingsStore.getTableName() == null || statsSettingsStore.getTableName().isEmpty()) {
			exception.add(ValidationConstant.Message.SINGLE_KEY_MANDATORY, SQLNoSQL.TABLE);
		}

		if (statsSettingsStore.getEndPoint() == null || statsSettingsStore.getEndPoint().isEmpty()) {
			exception.add(ValidationConstant.Message.SINGLE_KEY_MANDATORY, StatsSettings.END_POINT);
		}

		if (statsSettingsStore.getConstantNames() == null && statsSettingsStore.getConstantValues() != null) {
			exception.add(ValidationConstant.Message.IS_MANDATORY_WHEN_PRESENT, StatsSettings.CONSTANT_NAMES, StatsSettings.CONSTANT_VALUES);
		} else if (statsSettingsStore.getConstantNames() != null && statsSettingsStore.getConstantValues() == null) {
			exception.add(ValidationConstant.Message.IS_MANDATORY_WHEN_PRESENT, StatsSettings.CONSTANT_VALUES, StatsSettings.CONSTANT_NAMES);
		} else if (statsSettingsStore.getConstantNames() != null && statsSettingsStore.getConstantValues() != null
		        && statsSettingsStore.getConstantNames().size() != statsSettingsStore.getConstantValues().size()) {
			exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, StatsSettings.CONSTANT_NAMES, StatsSettings.CONSTANT_VALUES);
		}

		if (statsSettingsStore.getRetryCount() <= 0) {
			exception.add(ValidationConstant.Message.INVALID_VALUE_FOR, statsSettingsStore.getRetryCount(), General.MAX_RETRY_COUNT);
		}

		if (statsSettingsStore.getRetryDelay() <= 0) {
			exception.add(ValidationConstant.Message.INVALID_VALUE_FOR, statsSettingsStore.getRetryDelay(), General.RETRY_DELAY);
		}

		if (statsSettingsStore.getStatsType() == null) {
			exception.add(ValidationConstant.Message.KEY_IS_INVALID, StatsSettings.STATS_TYPE);
		} else if (statsSettingsStore.getStatsType().equals(StatsType.NONE)) {
			exception.add(ValidationConstant.Message.INVALID_FOR_GLOBAL_STATS_SETTINGS, StatsType.NONE, StatsSettings.STATS_TYPE);
		}

		if (exception.getNumberOfExceptions() > 0) {
			throw exception;
		}
	}
}
