package com.anvizent.elt.core.spark.operation.schema.validator;

import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.Join;
import com.anvizent.elt.core.spark.operation.config.bean.JoinConfigBean;
import com.anvizent.elt.core.spark.schema.validator.SchemaValidator;
import com.anvizent.elt.core.spark.util.StructureUtil;

/**
 * @author Hareen Bejjanki
 *
 */
public class JoinSchemaValidator extends SchemaValidator {
	private static final long serialVersionUID = 1L;

	@Override
	public void validate(ConfigBean configBean, int sourceIndex, LinkedHashMap<String, AnvizentDataType> structure,
	        InvalidConfigException invalidConfigException) {
		JoinConfigBean joinConfigBean = (JoinConfigBean) configBean;

		if (sourceIndex == 0) {
			StructureUtil.fieldsNotInSchema(Join.LEFT_HAND_SIDE_FIELDS, joinConfigBean.getLHSFields(), structure, false, invalidConfigException);
		} else {
			StructureUtil.fieldsNotInSchema(Join.RIGHT_HAND_SIDE_FIELDS, joinConfigBean.getRHSFields(), structure, false, invalidConfigException);
		}
	}

}
