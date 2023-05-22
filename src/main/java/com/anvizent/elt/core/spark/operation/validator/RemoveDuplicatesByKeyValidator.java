package com.anvizent.elt.core.spark.operation.validator;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.RemoveDuplicatesByKey;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.operation.config.bean.RemoveDuplicatesByKeyConfigBean;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public class RemoveDuplicatesByKeyValidator extends Validator {

	private static final long serialVersionUID = 1L;

	public RemoveDuplicatesByKeyValidator(Factory factory) {
		super(factory);
	}

	@Override
	public ConfigBean validateFactoryConfig(LinkedHashMap<String, String> configs, ConfigAndMappingConfigBeans configAndMappingConfigBeans)
	        throws InvalidConfigException, ImproperValidationException, UnsupportedException {
		RemoveDuplicatesByKeyConfigBean configBean = new RemoveDuplicatesByKeyConfigBean();

		validateMandatoryField(configs, RemoveDuplicatesByKey.KEY_FIELDS);

		setConfigBean(configBean, configs);

		return configBean;
	}

	private void setConfigBean(RemoveDuplicatesByKeyConfigBean configBean, LinkedHashMap<String, String> configs) throws ImproperValidationException {
		ArrayList<String> keyFields = ConfigUtil.getArrayList(configs, RemoveDuplicatesByKey.KEY_FIELDS, exception);

		configBean.setKeyFields(keyFields);
	}
}
