package com.anvizent.elt.core.spark.operation.validator;

import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.operation.config.bean.SequenceConfigBean;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public class SequenceValidator extends Validator {

	private static final long serialVersionUID = 1L;

	public SequenceValidator(Factory factory) {
		super(factory);
	}

	@Override
	public ConfigBean validateFactoryConfig(LinkedHashMap<String, String> configs, ConfigAndMappingConfigBeans configAndMappingConfigBeans)
	        throws InvalidConfigException, ImproperValidationException, UnsupportedException {
		validateMandatoryFields(configs, General.FIELDS);

		SequenceConfigBean configBean = new SequenceConfigBean();
		setConfigBean(configBean, configs);
		validateConfigBean(configBean, configs);
		return configBean;
	}

	private void setConfigBean(SequenceConfigBean configBean, LinkedHashMap<String, String> configs)
	        throws ImproperValidationException, InvalidConfigException {
		configBean.setFields(ConfigUtil.getArrayList(configs, General.FIELDS, exception));
		configBean.setFieldIndexes(ConfigUtil.getArrayListOfIntegers(configs, General.FIELD_INDEXES, exception));
		configBean.setInitialValues(ConfigUtil.getArrayListOfIntegers(configs, Operation.Sequence.INITIAL_VALUES, exception));
	}

	private void validateConfigBean(SequenceConfigBean configBean, LinkedHashMap<String, String> configs) throws ImproperValidationException {
		if (configBean.getFieldIndexes() != null && configBean.getFieldIndexes().size() != configBean.getFields().size()) {
			exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, General.FIELDS, General.FIELD_INDEXES);
		}

		if (configBean.getInitialValues() != null && configBean.getInitialValues().size() != configBean.getFields().size()) {
			exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, General.FIELDS, Operation.Sequence.INITIAL_VALUES);
		}
	}

}
