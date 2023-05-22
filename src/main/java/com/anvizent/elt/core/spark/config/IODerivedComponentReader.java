package com.anvizent.elt.core.spark.config;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.SeekDetails;
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
public class IODerivedComponentReader extends DerivedComponentReader {

	private static final long serialVersionUID = 1L;

	private BufferedReader configsReader;
	private String currentLine = null;

	@Override
	public DerivedComponentReader reset() {
		configsReader = null;
		currentLine = null;
		seek.set(0);
		return this;
	}

	@Override
	public DerivedComponentReader setConfigSource(String configSource) throws ConfigurationReadingException, InvalidInputForConfigException {
		try {
			source = configSource;
			configsReader = IOConfigurationReaderUtil.getConfigReader(configSource);
			readNextLine();
			componentName = IOConfigurationReaderUtil.getComponentName(configsReader, currentLine, seek);
			readNextLine();
		} catch (IOException ioException) {
			throw new ConfigurationReadingException(ioException.getMessage(), ioException);
		}

		return this;
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
	public String getComponentName() {
		return componentName;
	}

	@Override
	public ComponentConfiguration readNextConfiguration()
	        throws ConfigurationReadingException, InvalidInputForConfigException, ConfigReferenceNotFoundException {
		while (currentLine != null) {
			seek.add(1);

			if (!ConfigurationReaderUtil.isCommentOrEmptyLine(currentLine)) {
				IOConfigurationReaderUtil.validateConfigContainsSpace(currentLine, seek);

				if (currentLine.contains(Constants.General.PROPERTY_COMMENT)) {
					currentLine = currentLine.substring(0, currentLine.indexOf(Constants.General.PROPERTY_COMMENT));
				}

				try {
					int initialSeek = seek.get();
					LinkedHashMap<String, String> configs = IOConfigurationReaderUtil.getDerivedConfig(configsReader, currentLine, seek, source);
					ComponentConfiguration componentConfiguration = new ComponentConfiguration(currentLine,
					        new SeekDetails(initialSeek, seek.get(), getSeekName(), source), configs);
					readNextLine();

					return componentConfiguration;
				} catch (IOException exception) {
					throw new ConfigurationReadingException(exception.getMessage(), exception);
				}

			} else {
				readNextLine();
			}
		}

		return null;
	}
}
