package com.anvizent.elt.core.spark.operation.validator;

import java.util.LinkedHashMap;

import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.operation.config.bean.SQLLookUpConfigBean;
import com.anvizent.elt.core.spark.operation.config.bean.SQLRetrievalConfigBean;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class SQLLookUpValidator extends SQLRetrievalValidator {

	private static final long serialVersionUID = 1L;

	public SQLLookUpValidator(Factory factory) {
		super(factory);
	}

	@Override
	protected void setSQLRetrievalConfigBean(SQLRetrievalConfigBean configBean, LinkedHashMap<String, String> configs) {
		SQLLookUpConfigBean sqlLookUpConfigBean = (SQLLookUpConfigBean) configBean;

		sqlLookUpConfigBean.setLimitTo1(ConfigUtil.getBoolean(configs, Operation.General.LIMIT_TO_1, exception));
	}

	@Override
	protected void validateSQLRetrievalConfigBean(SQLRetrievalConfigBean configBean, LinkedHashMap<String, String> configs) {
	}

}
