package com.anvizent.elt.core.spark.reader;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.SeekDetails;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.listener.common.store.JobSettingStore;
import com.anvizent.elt.core.spark.config.ComponentConfiguration;
import com.anvizent.elt.core.spark.config.ConfigurationReader;
import com.anvizent.elt.core.spark.config.ConfigurationReaderBuilder;
import com.anvizent.elt.core.spark.config.ConfigurationReadingException;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General.JobSettings;
import com.anvizent.elt.core.spark.constant.Constants.ARGSConstant;
import com.anvizent.elt.core.spark.exception.ConfigReferenceNotFoundException;
import com.anvizent.elt.core.spark.exception.InvalidConfigurationReaderProperty;
import com.anvizent.elt.core.spark.exception.InvalidInputForConfigException;
import com.anvizent.elt.core.spark.exception.JobSettingsException;
import com.anvizent.elt.core.spark.exception.StatsSettingsException;

/**
 * @author Hareen Bejjanki
 *
 */
public class JobSettingsReader {

	public static JobSettingStore getJobSettingsStore(LinkedHashMap<String, ArrayList<String>> arguments)
	        throws UnsupportedEncodingException, ConfigurationReadingException, InvalidConfigurationReaderProperty, InvalidInputForConfigException,
	        ConfigReferenceNotFoundException, StatsSettingsException, ImproperValidationException {
		if (!arguments.containsKey(ARGSConstant.JOB_SETTINGS) || arguments.get(ARGSConstant.JOB_SETTINGS).get(0) == null
		        || arguments.get(ARGSConstant.JOB_SETTINGS).get(0).isEmpty()) {
			return null;
		} else {
			ConfigurationReader jobSettingConfigurationReader = new ConfigurationReaderBuilder(arguments, null, arguments.get(ARGSConstant.JOB_SETTINGS).get(0),
			        General.EOJS).build();
			return createJobSettingsStore(jobSettingConfigurationReader.getNextConfiguration());
		}
	}

	public static JobSettingStore createJobSettingsStore(ComponentConfiguration componentConfiguration) {
		LinkedHashMap<String, String> jobSettingConfigurations = componentConfiguration.getConfiguration();

		JobSettingsException jobSettingsException = createJobSettingsException(componentConfiguration.getSeekDetails());

		Long jobDetailsId = ConfigUtil.getLong(jobSettingConfigurations, JobSettings.JOB_DETAILS_ID, jobSettingsException);
		String jobDetailsURL = ConfigUtil.getString(jobSettingConfigurations, JobSettings.JOB_DETAILS_URL);
		String executorDetailsURL = ConfigUtil.getString(jobSettingConfigurations, JobSettings.EXECUTOR_DETAILS_URL);
		String applicationEndTimeURL = ConfigUtil.getString(jobSettingConfigurations, JobSettings.APPLICATION_END_TIME_URL);
		String appDBName = ConfigUtil.getString(jobSettingConfigurations, JobSettings.APP_DB_NAME);
		String clientId = ConfigUtil.getString(jobSettingConfigurations, JobSettings.CLIENT_ID);
		String hostName = ConfigUtil.getString(jobSettingConfigurations, JobSettings.HOST_NAME);
		String portNumber = ConfigUtil.getString(jobSettingConfigurations, JobSettings.PORT_NUMBER);
		String userName = ConfigUtil.getString(jobSettingConfigurations, JobSettings.USERNAME);
		String password = ConfigUtil.getString(jobSettingConfigurations, JobSettings.PASSWORD);
		String privateKey = ConfigUtil.getString(jobSettingConfigurations, JobSettings.PRIVATE_KEY);
		String iv = ConfigUtil.getString(jobSettingConfigurations, JobSettings.IV);
		Integer retryCount = ConfigUtil.getInteger(jobSettingConfigurations, General.MAX_RETRY_COUNT, jobSettingsException, General.DEFAULT_MAX_RETRY_COUNT);
		Long retryDelay = ConfigUtil.getLong(jobSettingConfigurations, General.RETRY_DELAY, jobSettingsException, General.DEFAULT_RETRY_DELAY);
		String initiatorTimeZone = ConfigUtil.getString(jobSettingConfigurations, JobSettings.IV);

		JobSettingStore jobSettingStore = new JobSettingStore(jobDetailsId, jobDetailsURL, executorDetailsURL, applicationEndTimeURL, appDBName, clientId,
		        hostName, portNumber, userName, password, privateKey, iv, retryCount, retryDelay, initiatorTimeZone);

		// TODO validate

		return jobSettingStore;
	}

	private static JobSettingsException createJobSettingsException(SeekDetails seekDetails) {
		JobSettingsException jobSettingsException = new JobSettingsException();

		jobSettingsException.setComponent(JobSettings.JOB_SETTINGS);
		jobSettingsException.setComponentName(JobSettings.JOB_SETTINGS_CONFIG_NAME);
		jobSettingsException.setSeekDetails(seekDetails);

		return jobSettingsException;
	}

}
