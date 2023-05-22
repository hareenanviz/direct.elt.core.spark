package com.anvizent.elt.core.spark.operation.schema.validator;

import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.ResultFetcher;
import com.anvizent.elt.core.spark.operation.config.bean.ResultFetcherConfigBean;
import com.anvizent.elt.core.spark.schema.validator.SchemaValidator;
import com.anvizent.elt.core.spark.util.StructureUtil;

/**
 * @author Hareen Bejjanki
 *
 */
public class ResultFetcherSchemaValidator extends SchemaValidator {
	private static final long serialVersionUID = 1L;

	@Override
	public void validate(ConfigBean configBean, int sourceIndex, LinkedHashMap<String, AnvizentDataType> structure,
	        InvalidConfigException invalidConfigException) {
		ResultFetcherConfigBean resultFetcherConfigBean = (ResultFetcherConfigBean) configBean;

		StructureUtil.multiLevelfieldsNotInSchema(ResultFetcher.METHOD_ARGUMENT_FIELDS, resultFetcherConfigBean.getMethodArgumentFields(), structure,
		        invalidConfigException);
		StructureUtil.fieldsInSchema(ResultFetcher.RETURN_FIELDS, resultFetcherConfigBean.getReturnFields(), structure, invalidConfigException, true);
	}

}
