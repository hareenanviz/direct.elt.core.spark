package com.anvizent.elt.core.spark.mapping.validator;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Mapping;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Mapping.Coerce;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant.Message;
import com.anvizent.elt.core.spark.mapping.config.bean.CoerceConfigBean;

/**
 * @author Hareen Bejjanki
 *
 */
public class CoerceValidator extends MappingValidator {
	private static final long serialVersionUID = 1L;

	@Override
	public MappingConfigBean validateAndSetBean(LinkedHashMap<String, String> configs) throws ImproperValidationException, InvalidConfigException {
		ArrayList<String> fields = ConfigUtil.getArrayList(configs, Coerce.COERCE_FIELDS, exception);
		ArrayList<Class<?>> types = ConfigUtil.getArrayListOfClassFromCSVRecord(configs, Coerce.COERCE_TO_TYPE, exception);
		ArrayList<String> formats = ConfigUtil.getArrayList(configs, Coerce.COERCE_TO_FORMAT, exception);
		ArrayList<Integer> precisions = ConfigUtil.getArrayListOfIntegers(configs, Coerce.COERCE_DECIMAL_PRECISION, exception);
		ArrayList<Integer> scales = ConfigUtil.getArrayListOfIntegers(configs, Coerce.COERCE_DECIMAL_SCALE, exception);

		if (fields == null || fields.isEmpty()) {
			addException(Message.SINGLE_KEY_MANDATORY, Mapping.Coerce.COERCE_FIELDS);
		}

		if (types == null || types.isEmpty()) {
			addException(Message.SINGLE_KEY_MANDATORY, Mapping.Coerce.COERCE_TO_TYPE);
		}

		if (fields != null && types != null) {
			if (fields.size() != types.size()) {
				addException(Message.SIZE_SHOULD_MATCH, Mapping.Coerce.COERCE_FIELDS, Mapping.Coerce.COERCE_TO_TYPE);
			}
		}

		if (formats != null && fields != null) {
			if (fields.size() != formats.size()) {
				addException(Message.SIZE_SHOULD_MATCH, Mapping.Coerce.COERCE_FIELDS, Mapping.Coerce.COERCE_TO_FORMAT);
			}
		}

		if (precisions != null) {
			if (precisions.size() != fields.size()) {
				addException(Message.SIZE_SHOULD_MATCH, Mapping.Coerce.COERCE_FIELDS, Mapping.Coerce.COERCE_DECIMAL_PRECISION);
			}
		}

		if (scales != null) {
			if (scales.size() != fields.size()) {
				addException(Message.SIZE_SHOULD_MATCH, Mapping.Coerce.COERCE_FIELDS, Mapping.Coerce.COERCE_DECIMAL_SCALE);
			}
		}

		return new CoerceConfigBean(fields, types, formats, precisions, scales);
	}

	@Override
	public String[] getConfigsList() throws ImproperValidationException {
		return new String[] { Coerce.COERCE_FIELDS, Coerce.COERCE_TO_TYPE, Coerce.COERCE_TO_FORMAT, Coerce.COERCE_DECIMAL_PRECISION,
		        Coerce.COERCE_DECIMAL_SCALE };
	}

}
