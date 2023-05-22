package com.anvizent.elt.core.spark.operation.validator;

import java.util.LinkedHashMap;

import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.operation.config.bean.SQLFetcherConfigBean;
import com.anvizent.elt.core.spark.operation.config.bean.SQLRetrievalConfigBean;

/**
 * @author Hareen Bejjanki
 *
 */
public class SQLFetcherValidator extends SQLRetrievalValidator {

	private static final long serialVersionUID = 1L;

	public SQLFetcherValidator(Factory factory) {
		super(factory);
	}

	@Override
	protected void setSQLRetrievalConfigBean(SQLRetrievalConfigBean configBean, LinkedHashMap<String, String> configs) throws InvalidConfigException {
		SQLFetcherConfigBean sqlFetcherConfigBean = (SQLFetcherConfigBean) configBean;

		sqlFetcherConfigBean.setMaxFetchLimit(ConfigUtil.getInteger(configs, Operation.General.MAX_FETCH_LIMIT, exception));
	}

	@Override
	protected void validateSQLRetrievalConfigBean(SQLRetrievalConfigBean configBean, LinkedHashMap<String, String> configs) {
		SQLFetcherConfigBean fetcherConfigBean = (SQLFetcherConfigBean) configBean;

		if (fetcherConfigBean.getMaxFetchLimit() == null) {
			exception.add(ValidationConstant.Message.SINGLE_KEY_MANDATORY, Operation.General.MAX_FETCH_LIMIT);
		} else if (fetcherConfigBean.getMaxFetchLimit() <= 0) {
			exception.add(ValidationConstant.Message.INVALID_INTEGERS, fetcherConfigBean.getMaxFetchLimit().toString(), Operation.General.MAX_FETCH_LIMIT);
		}
	}

}
