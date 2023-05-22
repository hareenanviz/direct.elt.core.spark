package com.anvizent.elt.core.spark.config;

import java.io.UnsupportedEncodingException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Set;

import com.anvizent.elt.core.lib.config.bean.SeekDetails;
import com.anvizent.elt.core.spark.constant.Constants.ARGSConstant;
import com.anvizent.elt.core.spark.constant.Constants.ExceptionMessage;
import com.anvizent.elt.core.spark.exception.ConfigReferenceNotFoundException;
import com.anvizent.elt.core.spark.exception.InvalidConfigurationReaderProperty;
import com.anvizent.elt.core.spark.exception.InvalidInputForConfigException;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class DerivedComponentsReader {

	private ArrayList<String> derivedConfigSources;
	private LinkedHashMap<String, ArrayList<String>> arguments;
	private Set<String> factories;

	public DerivedComponentsReader(LinkedHashMap<String, ArrayList<String>> arguments, Set<String> factories)
			throws InvalidConfigurationReaderProperty, UnsupportedEncodingException {
		this.derivedConfigSources = arguments.get(ARGSConstant.DERIVED_COMPONENT_CONFIGS);
		this.arguments = arguments;
		this.factories = factories;
	}

	public LinkedHashMap<String, DerivedComponentConfiguration> getDerivedComponents() throws InvalidConfigurationReaderProperty, UnsupportedEncodingException,
			ConfigurationReadingException, InvalidInputForConfigException, ConfigReferenceNotFoundException {
		if (derivedConfigSources == null) {
			return null;
		}

		LinkedHashMap<String, DerivedComponentConfiguration> derivedComponentConfigurations = new LinkedHashMap<>();
		DerivedComponentReader derivedComponentReader = getDerivedComponentReader();

		for (String derivedConfigsSource : derivedConfigSources) {
			DerivedComponentConfiguration derivedComponent = getDerivedComponent(derivedComponentReader, derivedConfigsSource);
			if (!derivedComponentConfigurations.containsKey(derivedComponent.getConfigurationName())) {
				derivedComponentConfigurations.put(derivedComponent.getConfigurationName(), derivedComponent);
			} else {
				throw new ConfigurationReadingException("Duplicate derived component name '" + derivedComponent.getConfigurationName() + "'");
			}
		}

		return derivedComponentConfigurations;
	}

	private DerivedComponentReader getDerivedComponentReader()
			throws InvalidConfigurationReaderProperty, UnsupportedEncodingException, ConfigurationReadingException {
		DerivedComponentReaderBuilder builder = new DerivedComponentReaderBuilder();

		if (arguments.containsKey(ARGSConstant.PRIVATE_KEY)) {
			builder.setEncryptionUtility(arguments.get(ARGSConstant.PRIVATE_KEY).get(0), arguments.get(ARGSConstant.IV).get(0));
		}

		if (arguments.containsKey(ARGSConstant.JDBC_URL)) {
			builder.setRDBMSConnection(arguments.get(ARGSConstant.JDBC_URL).get(0), arguments.get(ARGSConstant.DRIVER).get(0),
					arguments.get(ARGSConstant.USER_NAME).get(0), arguments.get(ARGSConstant.PASSWORD).get(0));
		}

		return builder.build();
	}

	private DerivedComponentConfiguration getDerivedComponent(DerivedComponentReader derivedComponentReader, String configSource)
			throws ConfigurationReadingException, InvalidInputForConfigException, ConfigReferenceNotFoundException {
		DerivedComponentConfiguration derivedComponentConfiguration = new DerivedComponentConfiguration();

		derivedComponentReader.reset().setConfigSource(configSource);

		if (factories.contains(derivedComponentReader.getComponentName())) {
			throw new InvalidInputForConfigException(
					MessageFormat.format(ExceptionMessage.INVALID_CONFIGURATION_KEY, derivedComponentReader.componentName) + "for Derived Component.");
		}

		derivedComponentConfiguration.setConfigurationName(derivedComponentReader.getComponentName());

		ComponentConfiguration config = null;
		while ((config = derivedComponentReader.readNextConfiguration()) != null) {
			derivedComponentConfiguration.addConfiguration(config);
		}

		derivedComponentConfiguration.setSeekDetails(new SeekDetails(derivedComponentReader.getSeekName(), configSource));

		return derivedComponentConfiguration;
	}

}
