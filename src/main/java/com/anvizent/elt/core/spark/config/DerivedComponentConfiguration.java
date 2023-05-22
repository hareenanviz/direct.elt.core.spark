package com.anvizent.elt.core.spark.config;

import java.util.ArrayList;

import com.anvizent.elt.core.lib.config.bean.SeekDetails;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class DerivedComponentConfiguration {

	private String configurationName;
	private SeekDetails seekDetails;
	private ArrayList<ComponentConfiguration> configurations = new ArrayList<>();

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

	public ArrayList<ComponentConfiguration> getConfigurations() {
		return configurations;
	}

	public void addConfiguration(ComponentConfiguration configuration) {
		configurations.add(configuration);
	}

}
