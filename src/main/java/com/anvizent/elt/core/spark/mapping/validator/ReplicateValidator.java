package com.anvizent.elt.core.spark.mapping.validator;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Mapping.Replicate;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant.Message;
import com.anvizent.elt.core.spark.mapping.config.bean.ReplicateConfigBean;

/**
 * @author Hareen Bejjanki
 *
 */
public class ReplicateValidator extends MappingValidator {

	private static final long serialVersionUID = 1L;

	@Override
	public MappingConfigBean validateAndSetBean(LinkedHashMap<String, String> configs) throws ImproperValidationException, InvalidConfigException {
		ArrayList<String> fields = ConfigUtil.getArrayList(configs, Replicate.REPLICATE_FIELDS, exception);
		ArrayList<String> toFields = ConfigUtil.getArrayList(configs, Replicate.REPLICATE_TO_FIELDS, exception);
		ArrayList<Integer> positions = ConfigUtil.getArrayListOfIntegers(configs, Replicate.REPLICATE_POSITIONS, exception);

		if (fields == null) {
			addException(Message.SINGLE_KEY_MANDATORY, Replicate.REPLICATE_FIELDS);
		}

		if (toFields == null) {
			addException(Message.SINGLE_KEY_MANDATORY, Replicate.REPLICATE_TO_FIELDS);
		} else if (fields != null) {
			if (fields.size() != toFields.size()) {
				addException(Message.SIZE_SHOULD_MATCH, Replicate.REPLICATE_FIELDS, Replicate.REPLICATE_TO_FIELDS);
			}
		}

		if (positions == null) {
			addException(Message.SINGLE_KEY_MANDATORY, Replicate.REPLICATE_POSITIONS);
		} else if (fields != null) {
			if (fields.size() != positions.size()) {
				addException(Message.SIZE_SHOULD_MATCH, Replicate.REPLICATE_FIELDS, Replicate.REPLICATE_POSITIONS);
			}
		}

		return new ReplicateConfigBean(fields, toFields, positions);
	}

	@Override
	public String[] getConfigsList() throws ImproperValidationException {
		return new String[] { Replicate.REPLICATE_FIELDS, Replicate.REPLICATE_TO_FIELDS, Replicate.REPLICATE_POSITIONS };
	}

}
