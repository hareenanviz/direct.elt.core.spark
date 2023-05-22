package com.anvizent.elt.core.spark.config;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.spark.config.util.ConfigurationReaderUtil;
import com.anvizent.elt.core.spark.config.util.IOConfigurationReaderUtil;
import com.anvizent.elt.core.spark.constant.Constants;
import com.anvizent.elt.core.spark.constant.Constants.ExceptionMessage;
import com.anvizent.elt.core.spark.exception.ConfigReferenceNotFoundException;
import com.anvizent.elt.core.spark.exception.InvalidInputForConfigException;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class IOConfigurationReader extends ConfigurationReader {
	private static final long serialVersionUID = 1L;

	private final BufferedReader configsReader;
	private String currentLine = null;
	private final String endPoint;

	public IOConfigurationReader(String configsSourcePath, String valuesSourcePath, String globalValuesSourcePath,
	        LinkedHashMap<String, DerivedComponentConfiguration> derivedComponents, String endPoint) throws ConfigurationReadingException {
		super(configsSourcePath, IOConfigurationReaderUtil.getValuesProperties(valuesSourcePath),
		        IOConfigurationReaderUtil.getValuesProperties(globalValuesSourcePath), derivedComponents);

		try {
			this.endPoint = endPoint;
			configsReader = IOConfigurationReaderUtil.getConfigReader(configsSourcePath);
		} catch (IOException ioException) {
			throw new ConfigurationReadingException(ioException.getMessage(), ioException);
		}

		readNextLine();
	}

	@Override
	protected ComponentConfiguration getComponentConfiguration()
	        throws ConfigurationReadingException, ConfigReferenceNotFoundException, InvalidInputForConfigException {
		while (currentLine != null) {
			seek.add(1);

			if (!ConfigurationReaderUtil.isCommentOrEmptyLine(currentLine)) {
				IOConfigurationReaderUtil.validateConfigContainsSpace(currentLine, seek);

				if (currentLine.contains(Constants.General.PROPERTY_COMMENT)) {
					currentLine = currentLine.substring(0, currentLine.indexOf(Constants.General.PROPERTY_COMMENT));
				}

				try {
					int initialSeek = seek.get();
					LinkedHashMap<String, String> config = IOConfigurationReaderUtil.getConfig(currentLine, configsReader, seek, valuesProperties,
					        globalValuesProperties, endPoint, source);

					return getComponentConfiguration(config, currentLine, initialSeek, seek.get(), getSeekName(), source, true);
				} catch (IOException ioException) {
					throw new ConfigurationReadingException(ioException.getMessage(), ioException);
				}
			} else {
				readNextLine();
			}
		}

		return null;
	}

	private void readNextLine() throws ConfigurationReadingException {
		try {
			currentLine = configsReader.readLine();
		} catch (IOException ioException) {
			throw new ConfigurationReadingException(ioException.getMessage(), ioException);
		}
	}

	@Override
	public String getSeekName() {
		return ExceptionMessage.AT_LINE_NUMBER;
	}

	@Override
	protected void afterComponentConfigurationCreated() throws ConfigurationReadingException, ConfigReferenceNotFoundException {
		readNextLine();
	}
}