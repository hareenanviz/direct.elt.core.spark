package com.anvizent.elt.core.spark.filter.validator;

import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Filter.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant.Message;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.filter.config.bean.FilterByExpressionConfigBean;
import com.anvizent.elt.core.spark.filter.service.FilterService;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class FilterByExpressionValidator extends Validator {

	private static final long serialVersionUID = 1L;

	public FilterByExpressionValidator(Factory factory) {
		super(factory);
	}

	@Override
	public ConfigBean validateFactoryConfig(LinkedHashMap<String, String> configs, ConfigAndMappingConfigBeans configAndMappingConfigBeans)
	        throws InvalidConfigException, ImproperValidationException, UnsupportedException {
		FilterByExpressionConfigBean configBean = new FilterByExpressionConfigBean();

		setConfigBean(configBean, configs);
		validateConfigBean(configBean);

		return configBean;
	}

	private void setConfigBean(FilterByExpressionConfigBean configBean, LinkedHashMap<String, String> configs) throws ImproperValidationException {
		FilterService.setEmitStreams(configBean, configs, exception);
		configBean.setExpressions(ConfigUtil.getArrayList(configs, Operation.General.EXPRESSIONS, exception));
		configBean.setArgumentFields(ConfigUtil.getArrayList(configs, Operation.General.ARGUMENT_FIELDS, exception));
		configBean.setArgumentTypes(ConfigUtil.getArrayListOfClass(configs, Operation.General.ARGUMENT_TYPES, exception));
	}

	private void validateConfigBean(FilterByExpressionConfigBean configBean) throws ImproperValidationException {

		if (configBean.getExpressions() == null) {
			exception.add(ValidationConstant.Message.SINGLE_KEY_MANDATORY, Operation.General.EXPRESSIONS);
		} else if (configBean.getExpressions().size() > 1) {
			if (configBean.getEmitStreamNames().size() < configBean.getExpressions().size()
			        || (configBean.getEmitStreamNames().size() - configBean.getExpressions().size() > 1)) {
				exception.add(Message.COUNT_EQUAL_OR_PLUS_1, General.EMIT_STREAM_NAMES, Operation.General.EXPRESSIONS);
			}
		}

		if (configBean.getArgumentFields() == null) {
			exception.add(ValidationConstant.Message.SINGLE_KEY_MANDATORY, Operation.General.ARGUMENT_FIELDS);
		}

		if (configBean.getArgumentTypes() == null) {
			exception.add(ValidationConstant.Message.SINGLE_KEY_MANDATORY, Operation.General.ARGUMENT_TYPES);
		}

		if (configBean.getArgumentTypes() != null && configBean.getArgumentFields() != null
		        && configBean.getArgumentTypes().size() != configBean.getArgumentFields().size()) {
			exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, Operation.General.ARGUMENT_TYPES, Operation.General.ARGUMENT_FIELDS);
		}
	}
}
