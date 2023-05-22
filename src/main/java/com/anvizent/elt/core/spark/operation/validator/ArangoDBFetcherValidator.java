package com.anvizent.elt.core.spark.operation.validator;

import java.util.LinkedHashMap;

import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.operation.config.bean.ArangoDBFetcherConfigBean;
import com.anvizent.elt.core.spark.operation.config.bean.ArangoDBRetrievalConfigBean;

/**
 * @author Hareen Bejjanki
 *
 */
public class ArangoDBFetcherValidator extends ArangoDBRetrievalValidator {

	private static final long serialVersionUID = 1L;

	public ArangoDBFetcherValidator(Factory factory) {
		super(factory);
	}

	@Override
	protected void setArangoDBRetrievalConfigBean(ArangoDBRetrievalConfigBean configBean, LinkedHashMap<String, String> configs) throws InvalidConfigException {
		ArangoDBFetcherConfigBean arangoDBFetcherConfigBean = (ArangoDBFetcherConfigBean) configBean;

		arangoDBFetcherConfigBean.setMaxFetchLimit(ConfigUtil.getInteger(configs, Operation.General.MAX_FETCH_LIMIT, exception));
	}

	@Override
	protected void validateArangoDBRetrievalConfigBean(ArangoDBRetrievalConfigBean configBean, LinkedHashMap<String, String> configs) {
		ArangoDBFetcherConfigBean arangoDBFetcherConfigBean = (ArangoDBFetcherConfigBean) configBean;

		if (arangoDBFetcherConfigBean.getMaxFetchLimit() == null) {
			exception.add(ValidationConstant.Message.SINGLE_KEY_MANDATORY, Operation.General.MAX_FETCH_LIMIT);
		} else if (arangoDBFetcherConfigBean.getMaxFetchLimit() <= 0) {
			exception.add(ValidationConstant.Message.INVALID_INTEGERS, arangoDBFetcherConfigBean.getMaxFetchLimit().toString(),
			        Operation.General.MAX_FETCH_LIMIT);
		}
	}

}
