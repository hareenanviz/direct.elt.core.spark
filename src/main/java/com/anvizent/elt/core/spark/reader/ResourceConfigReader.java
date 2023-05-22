package com.anvizent.elt.core.spark.reader;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.SeekDetails;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.listener.common.store.ResourceConfig;
import com.anvizent.elt.core.spark.config.ComponentConfiguration;
import com.anvizent.elt.core.spark.config.ConfigurationReader;
import com.anvizent.elt.core.spark.config.ConfigurationReaderBuilder;
import com.anvizent.elt.core.spark.config.ConfigurationReadingException;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants;
import com.anvizent.elt.core.spark.constant.Constants.ARGSConstant;
import com.anvizent.elt.core.spark.exception.ConfigReferenceNotFoundException;
import com.anvizent.elt.core.spark.exception.InvalidConfigurationReaderProperty;
import com.anvizent.elt.core.spark.exception.InvalidInputForConfigException;
import com.anvizent.elt.core.spark.exception.StatsSettingsException;

/**
 * @author Hareen Bejjanki
 *
 */
public class ResourceConfigReader {

	public ResourceConfig getJobConfig(LinkedHashMap<String, ArrayList<String>> arguments)
	        throws ConfigurationReadingException, InvalidInputForConfigException, ConfigReferenceNotFoundException, UnsupportedEncodingException,
	        InvalidConfigurationReaderProperty, StatsSettingsException, ImproperValidationException {

		if (!arguments.containsKey(ARGSConstant.RESOURCE_CONFIG)) {
			return null;
		}

		ConfigurationReader jobConfigReader = new ConfigurationReaderBuilder(arguments, null, arguments.get(ARGSConstant.RESOURCE_CONFIG).get(0),
		        ConfigConstants.General.EORC).build();
		ResourceConfig jobConfig = createJobConfigStore(jobConfigReader.getNextConfiguration());

		return jobConfig;
	}

	private ResourceConfig createJobConfigStore(ComponentConfiguration componentConfiguration) throws StatsSettingsException, ImproperValidationException {
		LinkedHashMap<String, String> statsSettingsConfigurations = componentConfiguration.getConfiguration();

		InvalidConfigException exception = createJobConfigException(componentConfiguration.getSeekDetails());

		ResourceConfig jobConfig = new ResourceConfig();
		jobConfig.setRdbmsConfigLocation(ConfigUtil.getString(statsSettingsConfigurations, ConfigConstants.ResourceConfig.RDBMS_CONFIG_LOCATION));
		jobConfig.setException(exception);

		return jobConfig;
	}

	private InvalidConfigException createJobConfigException(SeekDetails seekDetails) {
		InvalidConfigException exception = new InvalidConfigException();

		exception.setComponent(ConfigConstants.ResourceConfig.RESOURCE_CONFIG_CONFIG_NAME);
		exception.setComponentName(ConfigConstants.ResourceConfig.RESOURCE_CONFIG);
		exception.setSeekDetails(seekDetails);

		return exception;
	}
}
