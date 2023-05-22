package com.anvizent.elt.core.spark.mapping.validator;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Mapping.Retain;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant.Message;
import com.anvizent.elt.core.spark.mapping.config.bean.RetainConfigBean;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class RetainValidator extends MappingValidator {

	private static final long serialVersionUID = 1L;

	@Override
	public MappingConfigBean validateAndSetBean(LinkedHashMap<String, String> configs) throws ImproperValidationException {
		ArrayList<String> retainFields = ConfigUtil.getArrayList(configs, Retain.RETAIN_FIELDS, exception);
		ArrayList<String> retainFieldsAs = ConfigUtil.getArrayList(configs, Retain.RETAIN_AS, exception);
		ArrayList<Integer> retainFieldsAt = ConfigUtil.getArrayListOfIntegers(configs, Retain.RETAIN_AT, exception);
		ArrayList<String> emitFields = ConfigUtil.getArrayList(configs, Retain.RETAIN_EMIT, exception);

		if (retainFields != null && emitFields != null) {
			addException(Message.EITHER_OF_THE_KEY_IS_MANDATORY, Retain.RETAIN_FIELDS, Retain.RETAIN_EMIT);
		}

		if (retainFields != null && retainFieldsAs != null && retainFieldsAs.size() != retainFields.size()) {
			addException(Message.SIZE_SHOULD_MATCH, Retain.RETAIN_FIELDS, Retain.RETAIN_AS);
		}

		if (retainFields != null && retainFieldsAt != null && retainFieldsAt.size() != retainFields.size()) {
			addException(Message.SIZE_SHOULD_MATCH, Retain.RETAIN_FIELDS, Retain.RETAIN_AT);
		}

		if ((retainFieldsAs != null && !retainFieldsAs.isEmpty()) || (retainFieldsAt != null && !retainFieldsAt.isEmpty())) {
			if (retainFields == null || retainFields.isEmpty()) {
				addException(Message.IS_MANDATORY_FOR_2, Retain.RETAIN_FIELDS, Retain.RETAIN_AS, Retain.RETAIN_AT);
			}

			if (emitFields != null && !emitFields.isEmpty()) {
				addException(Message.IS_INVALID_FOR_2, Retain.RETAIN_EMIT, Retain.RETAIN_AS, Retain.RETAIN_AT);
			}
		}

		return new RetainConfigBean(retainFields, retainFieldsAs, retainFieldsAt, emitFields);
	}

	@Override
	public String[] getConfigsList() throws ImproperValidationException {
		return new String[] { Retain.RETAIN_FIELDS, Retain.RETAIN_EMIT };
	}
}
