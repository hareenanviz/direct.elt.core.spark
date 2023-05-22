package com.anvizent.elt.core.spark.mapping.validator;

import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.AppendAt;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Mapping.Duplicate;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant.Message;
import com.anvizent.elt.core.spark.mapping.config.bean.DuplicateConfigBean;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class DuplicateValidator extends MappingValidator {

	private static final long serialVersionUID = 1L;

	@Override
	public MappingConfigBean validateAndSetBean(LinkedHashMap<String, String> configs) throws ImproperValidationException {
		boolean measureAsString = ConfigUtil.getBoolean(configs, Duplicate.DUPLICATE_MEASURE_AS_STRING, exception);
		String prefix = ConfigUtil.getString(configs, Duplicate.DUPLICATE_MEASURE_AS_STRING_PREFIX);
		String suffix = ConfigUtil.getString(configs, Duplicate.DUPLICATE_MEASURE_AS_STRING_SUFFIX);
		AppendAt appendAt = AppendAt.getInstance(ConfigUtil.getString(configs, Duplicate.DUPLICATE_MEASURE_AS_STRING_APPEND_AT));

		if (measureAsString) {
			if (prefix == null && suffix == null) {
				addException(Message.EITHER_OF_THE_KEY_IS_MANDATORY, Duplicate.DUPLICATE_MEASURE_AS_STRING_PREFIX,
						Duplicate.DUPLICATE_MEASURE_AS_STRING_SUFFIX);
			}

			if (appendAt == null) {
				addException(Message.KEY_IS_INVALID, Duplicate.DUPLICATE_MEASURE_AS_STRING_APPEND_AT);
			}
		}

		return new DuplicateConfigBean(measureAsString, prefix, suffix, appendAt);
	}

	@Override
	public String[] getConfigsList() throws ImproperValidationException {
		return new String[] { Duplicate.DUPLICATE_MEASURE_AS_STRING, Duplicate.DUPLICATE_MEASURE_AS_STRING_PREFIX, Duplicate.DUPLICATE_MEASURE_AS_STRING_SUFFIX,
				Duplicate.DUPLICATE_MEASURE_AS_STRING_APPEND_AT };
	}

}
