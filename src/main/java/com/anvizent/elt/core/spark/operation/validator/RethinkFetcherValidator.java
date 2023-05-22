package com.anvizent.elt.core.spark.operation.validator;

import java.util.LinkedHashMap;

import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.operation.config.bean.RethinkFetcherConfigBean;
import com.anvizent.elt.core.spark.operation.config.bean.RethinkRetrievalConfigBean;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class RethinkFetcherValidator extends RethinkRetrievalValidator {

	private static final long serialVersionUID = 1L;

	public RethinkFetcherValidator(Factory factory) {
		super(factory);
	}

	@Override
	protected void setRetrievalConfigBean(RethinkRetrievalConfigBean configBean, LinkedHashMap<String, String> configs) {
		RethinkFetcherConfigBean rethinkFetcherConfigBean = (RethinkFetcherConfigBean) configBean;

		rethinkFetcherConfigBean.setMaxFetchLimit(ConfigUtil.getInteger(configs, Operation.General.MAX_FETCH_LIMIT, exception));
	}

	@Override
	protected void validateRetrievalConfigBean(RethinkRetrievalConfigBean configBean, LinkedHashMap<String, String> configs) {
		RethinkFetcherConfigBean rethinkFetcherConfigBean = (RethinkFetcherConfigBean) configBean;

		if (rethinkFetcherConfigBean.getMaxFetchLimit() == null) {
			exception.add(ValidationConstant.Message.SINGLE_KEY_MANDATORY, Operation.General.MAX_FETCH_LIMIT);
		} else if (rethinkFetcherConfigBean.getMaxFetchLimit() <= 0) {
			exception.add(ValidationConstant.Message.INVALID_INTEGERS, rethinkFetcherConfigBean.getMaxFetchLimit().toString(),
					Operation.General.MAX_FETCH_LIMIT);
		}
	}
}
