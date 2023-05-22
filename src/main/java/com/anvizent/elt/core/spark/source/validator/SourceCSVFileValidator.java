package com.anvizent.elt.core.spark.source.validator;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants;
import com.anvizent.elt.core.spark.constant.ConfigConstants.File;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Source.SourceCSV;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant;
import com.anvizent.elt.core.spark.constant.MalformedRows;
import com.anvizent.elt.core.spark.constant.SparkConstants.Options;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.source.config.bean.SourceCSVFileConfigBean;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public class SourceCSVFileValidator extends Validator {
	private static final long serialVersionUID = 1L;

	public SourceCSVFileValidator(Factory factory) {
		super(factory);
	}

	@Override
	public ConfigBean validateFactoryConfig(LinkedHashMap<String, String> configs, ConfigAndMappingConfigBeans configAndMappingConfigBeans)
	        throws InvalidConfigException, ImproperValidationException, UnsupportedException {
		SourceCSVFileConfigBean configBean = new SourceCSVFileConfigBean();

		validateAndSetPath(configBean, configs);

		validateAndSetSchema(configBean, configs);

		getOptions(configBean, configs);

		return configBean;
	}

	protected void validateAndSetSchema(SourceCSVFileConfigBean configBean, LinkedHashMap<String, String> configs)
	        throws ImproperValidationException, UnsupportedException, InvalidConfigException {
		Boolean inferSchema = ConfigUtil.getBoolean(configs, SourceCSV.INFER_SCHEMA, exception);
		ArrayList<String> fields = ConfigUtil.getArrayList(configs, General.FIELDS, exception);
		ArrayList<Class<?>> types = ConfigUtil.getArrayListOfClass(configs, General.TYPES, exception);
		ArrayList<Integer> precisions = ConfigUtil.getArrayListOfIntegers(configs, General.DECIMAL_PRECISIONS, exception);
		ArrayList<Integer> scales = ConfigUtil.getArrayListOfIntegers(configs, General.DECIMAL_SCALES, exception);

		if (inferSchema) {
			if (fields != null && fields.size() > 0) {
				exception.add(ValidationConstant.Message.INVALID_WHEN_OTHER_IS, General.FIELDS, SourceCSV.INFER_SCHEMA, Boolean.TRUE.toString());
			}

			if (types != null && types.size() > 0) {
				exception.add(ValidationConstant.Message.INVALID_WHEN_OTHER_IS, General.TYPES, SourceCSV.INFER_SCHEMA, Boolean.TRUE.toString());
			}
		} else if (types != null && types.size() > 0) {
			if (fields == null || fields.size() == 0) {
				exception.add(ValidationConstant.Message.IS_MANDATORY_WHEN_PRESENT, General.FIELDS, General.TYPES);
			} else if (types.size() != fields.size()) {
				exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, General.FIELDS, General.TYPES);
			} else {
				if (precisions != null && precisions.size() != fields.size()) {
					exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, General.DECIMAL_PRECISIONS, General.DECIMAL_SCALES);
				}
				if (scales != null && scales.size() != fields.size()) {
					exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, General.DECIMAL_SCALES, General.DECIMAL_SCALES);
				}
			}
		}

		if (exception.getNumberOfExceptions() == 0 && (inferSchema == null || !inferSchema)) {
			configBean.setStructure(fields, types, precisions, scales);
		}

	}

	protected void validateAndSetPath(SourceCSVFileConfigBean configBean, LinkedHashMap<String, String> configs) {
		validateMandatoryFields(configs, File.PATH);

		configBean.setPath(configs.get(ConfigConstants.File.PATH));
		configBean.addPrefix(configs.get(ConfigConstants.File.PART_FILES_PREFIX));
	}

	protected void getOptions(SourceCSVFileConfigBean configBean, LinkedHashMap<String, String> configs) throws ImproperValidationException {
		Map<String, String> options = new LinkedHashMap<String, String>();

		if (configs.get(SourceCSV.DELIMITER) != null && !configs.get(SourceCSV.DELIMITER).isEmpty()) {
			options.put(Options.SEP, configs.get(SourceCSV.DELIMITER));
		}

		if (configs.get(ConfigConstants.File.ENCODING) != null && !configs.get(ConfigConstants.File.ENCODING).isEmpty()) {
			options.put(Options.ENCODING, configs.get(ConfigConstants.File.ENCODING));
		}

		if (configs.get(SourceCSV.QUOTE) != null && !configs.get(SourceCSV.QUOTE).isEmpty()) {
			options.put(Options.QUOTE, configs.get(SourceCSV.QUOTE));
		}

		if (configs.get(ConfigConstants.File.ESCAPE_CHAR) != null && !configs.get(ConfigConstants.File.ESCAPE_CHAR).isEmpty()) {
			options.put(Options.ESCAPE, configs.get(ConfigConstants.File.ESCAPE_CHAR));
		}

		if (configs.get(ConfigConstants.File.COMMENT) != null && !configs.get(ConfigConstants.File.COMMENT).isEmpty()) {
			options.put(Options.COMMENT, configs.get(ConfigConstants.File.COMMENT));
		} else {
			options.put(ConfigConstants.File.COMMENT, ConfigConstants.File.DEFAULT_COMMENT);
		}

		if (configs.get(SourceCSV.HAS_HEADER) != null && !configs.get(SourceCSV.HAS_HEADER).isEmpty()) {
			options.put(Options.HEADER, configs.get(SourceCSV.HAS_HEADER));
		}

		if (configs.get(SourceCSV.INFER_SCHEMA) != null && !configs.get(SourceCSV.INFER_SCHEMA).isEmpty()) {
			options.put(Options.INFER_SCHEMA, configs.get(SourceCSV.INFER_SCHEMA));
		}

		if (configs.get(SourceCSV.IGNORE_LEADING_WHITE_SPACE) != null && !configs.get(SourceCSV.IGNORE_LEADING_WHITE_SPACE).isEmpty()) {
			options.put(Options.IGNORE_LEADING_WHITE_SPACE, configs.get(SourceCSV.IGNORE_LEADING_WHITE_SPACE));
		}

		if (configs.get(SourceCSV.IGNORE_TRAILING_WHITE_SPACE) != null && !configs.get(SourceCSV.IGNORE_TRAILING_WHITE_SPACE).isEmpty()) {
			options.put(Options.IGNORE_TRAILING_WHITE_SPACE, configs.get(SourceCSV.IGNORE_TRAILING_WHITE_SPACE));
		}

		// TODO Re think using cleansing
		if (configs.get(SourceCSV.NULL_VALUE) != null && !configs.get(SourceCSV.NULL_VALUE).isEmpty()) {
			options.put(Options.NULL_VALUE, configs.get(SourceCSV.NULL_VALUE));
		}

		if (configs.get(SourceCSV.MAX_COLUMNS) != null && !configs.get(SourceCSV.MAX_COLUMNS).isEmpty()) {
			options.put(Options.MAX_COLUMNS, configs.get(SourceCSV.MAX_COLUMNS));
		}

		if (configs.get(SourceCSV.MAX_CHARS_PER_COLUMN) != null && !configs.get(SourceCSV.MAX_CHARS_PER_COLUMN).isEmpty()) {
			options.put(Options.MAX_CHARS_PER_COLUMN, configs.get(SourceCSV.MAX_CHARS_PER_COLUMN));
		}

		MalformedRows malformedRows = MalformedRows.getInstance(configs.get(SourceCSV.MALFORMED_ROWS));
		options.put(Options.MODE, malformedRows.getOptionValue());

		if (configs.get(SourceCSV.MULTI_LINE) != null && !configs.get(SourceCSV.MULTI_LINE).isEmpty()) {
			options.put(Options.MULTI_LINE, configs.get(SourceCSV.MULTI_LINE));
		}

		if (configs.get(ConfigConstants.File.DATE_FORMAT) != null && !configs.get(ConfigConstants.File.DATE_FORMAT).isEmpty()) {
			options.put(Options.TIMESTAMP_FORMAT, configs.get(ConfigConstants.File.DATE_FORMAT));
		}

		configBean.setOptions(options);
	}
}
