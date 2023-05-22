package com.anvizent.elt.core.spark.config;

import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.SeekDetails;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ComponentConfiguration {

	private String configurationName;
	private SeekDetails seekDetails;
	private LinkedHashMap<String, String> configuration;

	public String getConfigurationName() {
		return configurationName;
	}

	public void setConfigurationName(String configurationName) {
		this.configurationName = configurationName;
	}

	public SeekDetails getSeekDetails() {
		return seekDetails;
	}

	public void setSeekDetails(SeekDetails seekDetails) {
		this.seekDetails = seekDetails;
	}

	public LinkedHashMap<String, String> getConfiguration() {
		return configuration;
	}

	public void setConfiguration(LinkedHashMap<String, String> configuration) {
		this.configuration = configuration;
	}

	public ComponentConfiguration(String configurationName, SeekDetails seekDetails, LinkedHashMap<String, String> configuration) {
		this.configurationName = configurationName;
		this.seekDetails = seekDetails;
		this.configuration = configuration;
	}
}