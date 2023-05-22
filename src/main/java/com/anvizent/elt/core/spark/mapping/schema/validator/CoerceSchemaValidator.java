package com.anvizent.elt.core.spark.mapping.schema.validator;

import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.mapping.config.bean.CoerceConfigBean;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class CoerceSchemaValidator implements MappingSchemaValidator {

	private static final long serialVersionUID = 1L;

	@Override
	public void validate(MappingConfigBean mappingConfigBean, LinkedHashMap<String, AnvizentDataType> structure, InvalidConfigException invalidConfigException)
	        throws InvalidConfigException {
		CoerceConfigBean configBean = (CoerceConfigBean) mappingConfigBean;

		// TODO
//		StructureUtil.fieldsNotInSchema(ConditionalReplacementCleansing.FIELDS, configBean.getFields(), structure, invalidConfigException);
//		StructureUtil.fieldsNotInSchema(ConditionalReplacementCleansing.REPLACEMENT_VALUES_BY_FIELDS, configBean.getReplacementValuesByFields(), structure,
//		        true, invalidConfigException);
//		StructureUtil.fieldsNotInSchema(CustomJavaExpressionCleansing.ARGUMENT_FIELDS, configBean.getArgumentFields(), structure, invalidConfigException);
//		StructureUtil.typesMissMatchInSchema(ConditionalReplacementCleansing.FIELDS, ConditionalReplacementCleansing.REPLACEMENT_VALUES_BY_FIELDS,
//		        configBean.getFields(), configBean.getReplacementValuesByFields(), structure, true, invalidConfigException);
	}

}
