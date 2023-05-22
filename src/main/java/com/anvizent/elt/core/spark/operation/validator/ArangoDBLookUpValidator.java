package com.anvizent.elt.core.spark.operation.validator;

import java.util.LinkedHashMap;

import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.operation.config.bean.ArangoDBLookUpConfigBean;
import com.anvizent.elt.core.spark.operation.config.bean.ArangoDBRetrievalConfigBean;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ArangoDBLookUpValidator extends ArangoDBRetrievalValidator {

	private static final long serialVersionUID = 1L;

	public ArangoDBLookUpValidator(Factory factory) {
		super(factory);
	}

	@Override
	protected void setArangoDBRetrievalConfigBean(ArangoDBRetrievalConfigBean configBean, LinkedHashMap<String, String> configs) {
		ArangoDBLookUpConfigBean arangoDBLookUpConfigBean = (ArangoDBLookUpConfigBean) configBean;

		arangoDBLookUpConfigBean.setLimitTo1(ConfigUtil.getBoolean(configs, Operation.General.LIMIT_TO_1, exception));
	}

	@Override
	protected void validateArangoDBRetrievalConfigBean(ArangoDBRetrievalConfigBean configBean, LinkedHashMap<String, String> configs) {
	}

}
