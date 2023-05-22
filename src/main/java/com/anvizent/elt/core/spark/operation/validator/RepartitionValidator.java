package com.anvizent.elt.core.spark.operation.validator;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.Repartition;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.operation.config.bean.RepartitionConfigBean;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public class RepartitionValidator extends Validator {

	private static final long serialVersionUID = 1L;

	public RepartitionValidator(Factory factory) {
		super(factory);
	}

	@Override
	public ConfigBean validateFactoryConfig(LinkedHashMap<String, String> configs, ConfigAndMappingConfigBeans configAndMappingConfigBeans)
	        throws InvalidConfigException, ImproperValidationException, UnsupportedException {
		RepartitionConfigBean configBean = new RepartitionConfigBean();

		validateAndSetConfigBean(configBean, configs);

		return configBean;
	}

	private void validateAndSetConfigBean(RepartitionConfigBean configBean, LinkedHashMap<String, String> configs) throws ImproperValidationException {
		ArrayList<String> keyFields = ConfigUtil.getArrayList(configs, Repartition.KEY_FIELDS, exception);
		Integer numberOfPartitions = ConfigUtil.getInteger(configs, Repartition.NUMBER_OF_PARTITIONS, exception);

		if ((keyFields == null || keyFields.isEmpty()) && numberOfPartitions == null) {
			exception.add(ValidationConstant.Message.EITHER_BOTH_OR_ONE_IS_MANDATORY, Repartition.KEY_FIELDS, Repartition.NUMBER_OF_PARTITIONS);
		}

		if (numberOfPartitions != null && numberOfPartitions <= 0) {
			exception.add(ValidationConstant.Message.INVALID_VALUE_FOR, numberOfPartitions, Repartition.NUMBER_OF_PARTITIONS);
		}

		configBean.setKeyFields(keyFields);
		configBean.setNumberOfPartitions(numberOfPartitions);
	}
}
