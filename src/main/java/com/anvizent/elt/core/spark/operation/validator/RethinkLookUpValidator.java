package com.anvizent.elt.core.spark.operation.validator;

import java.util.LinkedHashMap;

import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.operation.config.bean.RethinkLookUpConfigBean;
import com.anvizent.elt.core.spark.operation.config.bean.RethinkRetrievalConfigBean;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class RethinkLookUpValidator extends RethinkRetrievalValidator {

	private static final long serialVersionUID = 1L;

	public RethinkLookUpValidator(Factory factory) {
		super(factory);
	}

	@Override
	protected void validateRetrievalConfigBean(RethinkRetrievalConfigBean configBean, LinkedHashMap<String, String> configs) {
		RethinkLookUpConfigBean rethinkLookUpConfigBean = (RethinkLookUpConfigBean) configBean;

		rethinkLookUpConfigBean.setLimitTo1(ConfigUtil.getBoolean(configs, Operation.General.LIMIT_TO_1, exception));
	}

	@Override
	protected void setRetrievalConfigBean(RethinkRetrievalConfigBean configBean, LinkedHashMap<String, String> configs) {
	}
}
