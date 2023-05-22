package com.anvizent.elt.core.spark.mapping.validator;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Mapping.Constant;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant.Message;
import com.anvizent.elt.core.spark.mapping.config.bean.ConstantConfigBean;

/**
 * @author Hareen Bejjanki
 *
 */
public class ConstantValidator extends MappingValidator {

	private static final long serialVersionUID = 1L;

	@Override
	public MappingConfigBean validateAndSetBean(LinkedHashMap<String, String> configs) throws ImproperValidationException, InvalidConfigException {
		ArrayList<String> fields = ConfigUtil.getArrayList(configs, Constant.CONSTANT_FIELDS, exception);
		ArrayList<Class<?>> types = ConfigUtil.getArrayListOfClassFromCSVRecord(configs, Constant.CONSTANT_FIELDS_TYPES, exception);
		ArrayList<String> formats = ConfigUtil.getArrayList(configs, Constant.CONSTANT_FIELDS_FORMATS, exception);
		ArrayList<String> values = ConfigUtil.getArrayList(configs, Constant.CONSTANT_FIELDS_VALUES, exception);
		ArrayList<Integer> positions = ConfigUtil.getArrayListOfIntegers(configs, Constant.CONSTANT_FIELDS_POSITIONS, exception);
		ArrayList<Integer> precisions = ConfigUtil.getArrayListOfIntegers(configs, Constant.CONSTANT_DECIMAL_PRECISION, exception);
		ArrayList<Integer> scales = ConfigUtil.getArrayListOfIntegers(configs, Constant.CONSTANT_DECIMAL_SCALE, exception);

		if (fields == null || fields.isEmpty()) {
			addException(Message.SINGLE_KEY_MANDATORY, Constant.CONSTANT_FIELDS);
		}

		if (types == null || types.isEmpty()) {
			addException(Message.SINGLE_KEY_MANDATORY, Constant.CONSTANT_FIELDS_TYPES);
		} else if (fields != null) {
			if (fields.size() != types.size()) {
				addException(Message.SIZE_SHOULD_MATCH, Constant.CONSTANT_FIELDS, Constant.CONSTANT_FIELDS_TYPES);
			}
		}

		if (values == null || values.isEmpty()) {
			addException(Message.SINGLE_KEY_MANDATORY, Constant.CONSTANT_FIELDS_VALUES);
		} else if (fields != null) {
			if (fields.size() != values.size()) {
				addException(Message.SIZE_SHOULD_MATCH, Constant.CONSTANT_FIELDS, Constant.CONSTANT_FIELDS_VALUES);
			}
		}

		if (fields != null && positions != null && !positions.isEmpty() && fields.size() != positions.size()) {
			addException(Message.SIZE_SHOULD_MATCH, Constant.CONSTANT_FIELDS, Constant.CONSTANT_FIELDS_POSITIONS);
		}

		if (formats != null && fields != null) {
			if (fields.size() != formats.size()) {
				addException(Message.SIZE_SHOULD_MATCH, Constant.CONSTANT_FIELDS, Constant.CONSTANT_FIELDS_FORMATS);
			}
		}

		if (precisions != null) {
			if (precisions.size() != fields.size()) {
				addException(Message.SIZE_SHOULD_MATCH, Constant.CONSTANT_FIELDS, Constant.CONSTANT_DECIMAL_PRECISION);
			}
		}

		if (scales != null) {
			if (scales.size() != fields.size()) {
				addException(Message.SIZE_SHOULD_MATCH, Constant.CONSTANT_FIELDS, Constant.CONSTANT_DECIMAL_SCALE);
			}
		}

		return new ConstantConfigBean(fields, types, formats, values, positions, precisions, scales);
	}

	@Override
	public String[] getConfigsList() throws ImproperValidationException {
		return new String[] { Constant.CONSTANT_FIELDS, Constant.CONSTANT_FIELDS_TYPES, Constant.CONSTANT_FIELDS_FORMATS, Constant.CONSTANT_FIELDS_VALUES,
		        Constant.CONSTANT_FIELDS_POSITIONS, Constant.CONSTANT_DECIMAL_PRECISION, Constant.CONSTANT_DECIMAL_SCALE };
	}
}