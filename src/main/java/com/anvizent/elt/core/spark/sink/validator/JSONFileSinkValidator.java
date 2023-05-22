package com.anvizent.elt.core.spark.sink.validator;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.spark.sql.SaveMode;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.Compression;
import com.anvizent.elt.core.spark.constant.ConfigConstants;
import com.anvizent.elt.core.spark.constant.SparkConstants;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.sink.config.bean.FileSinkConfigBean;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public class JSONFileSinkValidator extends Validator {
	private static final long serialVersionUID = 1L;

	public JSONFileSinkValidator(Factory factory) {
		super(factory);
	}

	@Override
	public ConfigBean validateFactoryConfig(LinkedHashMap<String, String> configs, ConfigAndMappingConfigBeans configAndMappingConfigBeans)
	        throws InvalidConfigException, ImproperValidationException {
		FileSinkConfigBean configBean = new FileSinkConfigBean();

		validateAndSetPathAndOPtions(configBean, configs);

		return configBean;
	}

	protected void validateAndSetPathAndOPtions(FileSinkConfigBean configBean, LinkedHashMap<String, String> configs) {
		validateMandatoryFields(configs, ConfigConstants.File.PATH);
		configBean.setPath(ConfigUtil.getString(configs, ConfigConstants.File.PATH));

		setConfigBean(configBean, configs);
		setOptions(configBean, configs);
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
		Map<String, String> options = new LinkedHashMap<>();

		String timeZone = ConfigUtil.getString(configs, ConfigConstants.File.TIME_ZONE);
		if (timeZone != null) {
			options.put(SparkConstants.Options.TIME_ZONE, timeZone);
		}

		String dateFormat = ConfigUtil.getString(configs, ConfigConstants.File.DATE_FORMAT);
		if (dateFormat != null) {
			options.put(SparkConstants.Options.DATE_FORMAT, dateFormat);
		}

		String timestampFormat = ConfigUtil.getString(configs, ConfigConstants.File.TIMESTAMP_FORMAT);
		if (timestampFormat != null) {
			options.put(SparkConstants.Options.TIMESTAMP_FORMAT, timestampFormat);
		}

		String encoding = ConfigUtil.getString(configs, ConfigConstants.File.ENCODING);
		if (encoding != null) {
			options.put(SparkConstants.Options.ENCODING, encoding);
		}

		String lineSep = ConfigUtil.getString(configs, ConfigConstants.File.LINE_SEP);
		if (lineSep != null) {
			options.put(SparkConstants.Options.LINE_SEP, lineSep);
		}

		options.put(SparkConstants.Options.COMPRESSION,
		        ConfigUtil.getCompression(configs, ConfigConstants.File.COMPRESSION, exception, Compression.NONE).getValue());

		String ignoreNullFields = ConfigUtil.getString(configs, ConfigConstants.File.IGNORE_NULL_FIELDS);
		if (ignoreNullFields != null) {
			options.put(SparkConstants.Options.IGNORE_NULL_FIELDS, ignoreNullFields);
		}

		configBean.setOptions(options);
	}
}