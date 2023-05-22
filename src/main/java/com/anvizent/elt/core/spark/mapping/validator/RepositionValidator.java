package com.anvizent.elt.core.spark.mapping.validator;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Mapping.Reposition;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant.Message;
import com.anvizent.elt.core.spark.mapping.config.bean.RepositionConfigBean;

/**
 * @author Hareen Bejjanki
 *
 */
public class RepositionValidator extends MappingValidator {

	private static final long serialVersionUID = 1L;

	@Override
	public MappingConfigBean validateAndSetBean(LinkedHashMap<String, String> configs) throws ImproperValidationException, InvalidConfigException {
		ArrayList<String> fields = ConfigUtil.getArrayList(configs, Reposition.FIELDS, exception);
		ArrayList<Integer> positions = ConfigUtil.getArrayListOfIntegers(configs, Reposition.POSITIONS, exception);

		if (fields == null) {
			addException(Message.SINGLE_KEY_MANDATORY, Reposition.FIELDS);
		}
		if (positions == null) {
			addException(Message.SINGLE_KEY_MANDATORY, Reposition.POSITIONS);
		}
		if (fields != null && positions != null && fields.size() != positions.size()) {
			addException(Message.SIZE_SHOULD_MATCH, Reposition.FIELDS, Reposition.POSITIONS);
		}

		return new RepositionConfigBean(fields, positions);
	}

	@Override
	public String[] getConfigsList() throws ImproperValidationException {
		return new String[] { Reposition.FIELDS, Reposition.POSITIONS };
	}
}
