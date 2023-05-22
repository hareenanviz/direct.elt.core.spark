package com.anvizent.elt.core.spark.sink.validator;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.spark.sql.SaveMode;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants;
import com.anvizent.elt.core.spark.constant.ConfigConstants.File;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Sink.CSVSink;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Source.SourceCSV;
import com.anvizent.elt.core.spark.constant.SparkConstants;
import com.anvizent.elt.core.spark.constant.SparkConstants.Options;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.sink.config.bean.FileSinkConfigBean;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public class CSVFileSinkValidator extends Validator {
	private static final long serialVersionUID = 1L;

	public CSVFileSinkValidator(Factory factory) {
		super(factory);
	}

	@Override
	public ConfigBean validateFactoryConfig(LinkedHashMap<String, String> configs, ConfigAndMappingConfigBeans configAndMappingConfigBeans)
	        throws InvalidConfigException, ImproperValidationException {
		FileSinkConfigBean configBean = new FileSinkConfigBean();

		validateAndSetPathAndOptions(configBean, configs);

		return configBean;
	}

	protected void validateAndSetPathAndOptions(FileSinkConfigBean configBean, LinkedHashMap<String, String> configs) {
		validateMandatoryFields(configs, ConfigConstants.File.PATH);
		configBean.setPath(ConfigUtil.getString(configs, ConfigConstants.File.PATH));

		setOptions(configBean, configs);
		setConfigBean(configBean, configs);
	}

	protected void setConfigBean(FileSinkConfigBean configBean, LinkedHashMap<String, String> configs) {
		configBean.setSingleFile(ConfigUtil.getBoolean(configs, ConfigConstants.File.SINGLE_FILE, exception, false));

		try {
			configBean.setSaveMode(SaveMode.valueOf(ConfigUtil.getString(configs, ConfigConstants.File.SAVE_MODE)));
		} catch (IllegalArgumentException argumentException) {
			configBean.setSaveMode(null);
		}
	}

	protected void setOptions(FileSinkConfigBean configBean, LinkedHashMap<String, String> configs) {

		Map<String, String> options = new LinkedHashMap<String, String>();

		if (configs.get(SourceCSV.DELIMITER) != null && !configs.get(SourceCSV.DELIMITER).isEmpty()) {
			options.put(Options.SEP, configs.get(SourceCSV.DELIMITER));
		}

		if (configs.get(SourceCSV.QUOTE) != null && !configs.get(SourceCSV.QUOTE).isEmpty()) {
			options.put(Options.QUOTE, configs.get(SourceCSV.QUOTE));
		}

		if (configs.get(CSVSink.QUOTE_ALL) != null && !configs.get(CSVSink.QUOTE_ALL).isEmpty()) {
			options.put(Options.QUOTE_ALL, configs.get(CSVSink.QUOTE_ALL));
		}

		if (configs.get(ConfigConstants.File.ESCAPE_CHAR) != null && !configs.get(ConfigConstants.File.ESCAPE_CHAR).isEmpty()) {
			options.put(Options.ESCAPE, configs.get(ConfigConstants.File.ESCAPE_CHAR));
		}

		if (configs.get(CSVSink.ESCAPE_QUOTES) != null && !configs.get(CSVSink.ESCAPE_QUOTES).isEmpty()) {
			options.put(Options.ESCAPE, configs.get(CSVSink.ESCAPE_QUOTES));
		}

		String encoding = ConfigUtil.getString(configs, ConfigConstants.File.ENCODING);
		if (encoding != null) {
			options.put(SparkConstants.Options.ENCODING, encoding);
		}

		String lineSep = ConfigUtil.getString(configs, ConfigConstants.File.LINE_SEP);
		if (lineSep != null) {
			options.put(SparkConstants.Options.LINE_SEP, lineSep);
		}

		String charToEscapeQuoteEscaping = ConfigUtil.getString(configs, ConfigConstants.File.CHAR_TO_ESCAPE_QUOTE_ESCAPING);
		if (charToEscapeQuoteEscaping != null) {
			options.put(SparkConstants.Options.CHAR_TO_ESCAPE_QUOTE_ESCAPING, charToEscapeQuoteEscaping);
		}

		String emptyValue = ConfigUtil.getString(configs, ConfigConstants.File.EMPTY_VALUE);
		if (emptyValue != null) {
			options.put(SparkConstants.Options.EMPTY_VALUE, emptyValue);
		}

		if (configs.get(SourceCSV.HAS_HEADER) != null && !configs.get(SourceCSV.HAS_HEADER).isEmpty()) {
			options.put(Options.HEADER, configs.get(SourceCSV.HAS_HEADER));
		}

		if (configs.get(SourceCSV.NULL_VALUE) != null && !configs.get(SourceCSV.NULL_VALUE).isEmpty()) {
			options.put(Options.NULL_VALUE, configs.get(SourceCSV.NULL_VALUE));
		}

		if (configs.get(SourceCSV.IGNORE_LEADING_WHITE_SPACE) != null && !configs.get(SourceCSV.IGNORE_LEADING_WHITE_SPACE).isEmpty()) {
			options.put(Options.IGNORE_LEADING_WHITE_SPACE, configs.get(SourceCSV.IGNORE_LEADING_WHITE_SPACE));
		}

		if (configs.get(SourceCSV.IGNORE_TRAILING_WHITE_SPACE) != null && !configs.get(SourceCSV.IGNORE_TRAILING_WHITE_SPACE).isEmpty()) {
			options.put(Options.IGNORE_TRAILING_WHITE_SPACE, configs.get(SourceCSV.IGNORE_TRAILING_WHITE_SPACE));
		}

		if (configs.get(CSVSink.TIMESTAMP_FORMAT) != null && !configs.get(CSVSink.TIMESTAMP_FORMAT).isEmpty()) {
			options.put(Options.TIMESTAMP_FORMAT, configs.get(CSVSink.TIMESTAMP_FORMAT));
		}

		if (configs.get(CSVSink.DATE_FORMAT) != null && !configs.get(CSVSink.DATE_FORMAT).isEmpty()) {
			options.put(Options.DATE_FORMAT, configs.get(CSVSink.DATE_FORMAT));
		}

		if (configs.get(File.COMPRESSION) != null && !configs.get(File.COMPRESSION).isEmpty()) {
			options.put(Options.COMPRESSION, ConfigUtil.getCompression(configs, ConfigConstants.File.COMPRESSION, exception).getValue());
		}

		configBean.setOptions(options);
	}
}