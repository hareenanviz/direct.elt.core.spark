package com.anvizent.elt.core.spark.filter.validator;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Filter.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Filter.RegularExpression;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant.Message;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.filter.config.bean.FilterByRegExConfigBean;
import com.anvizent.elt.core.spark.filter.service.FilterService;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class FilterByRegExValidator extends Validator {

	private static final long serialVersionUID = 1L;

	public FilterByRegExValidator(Factory factory) {
		super(factory);
	}

	@Override
	public ConfigBean validateFactoryConfig(LinkedHashMap<String, String> configs, ConfigAndMappingConfigBeans configAndMappingConfigBeans)
	        throws InvalidConfigException, ImproperValidationException, UnsupportedException {
		FilterByRegExConfigBean configBean = new FilterByRegExConfigBean();

		setConfigBean(configBean, configs);
		validateConfigBean(configBean);

		return configBean;
	}

	private void setConfigBean(FilterByRegExConfigBean configBean, LinkedHashMap<String, String> configs) throws ImproperValidationException {
		ArrayList<String> fields = ConfigUtil.getArrayList(configs, RegularExpression.FIELDS, exception);
		ArrayList<String> regularExpressions = ConfigUtil.getArrayList(configs, RegularExpression.REGULAR_EXPRESSIONS, exception);

		configBean.setArgumentFields(fields);
		configBean.setRegExps(regularExpressions);
		configBean.setIgnoreRowIfNull(ConfigUtil.getBoolean(configs, RegularExpression.IGNORE_ROW_IF_NULL, exception));

		FilterService.setEmitStreams(configBean, configs, exception);
	}

	private void validateConfigBean(FilterByRegExConfigBean configBean) {
		if (configBean.getArgumentFields() == null || configBean.getArgumentFields().isEmpty()) {
			exception.add(Message.SINGLE_KEY_MANDATORY, RegularExpression.FIELDS);
		}
		if (configBean.getRegExps() == null || configBean.getRegExps().isEmpty()) {
			exception.add(Message.SINGLE_KEY_MANDATORY, RegularExpression.REGULAR_EXPRESSIONS);
		} else if (configBean.getRegExps().size() > 1) {
			if ((configBean.getEmitStreamNames().size() < configBean.getRegExps().size())
			        || (configBean.getEmitStreamNames().size() - configBean.getRegExps().size() > 1)) {
				exception.add(Message.COUNT_EQUAL_OR_PLUS_1, General.EMIT_STREAM_NAMES, RegularExpression.REGULAR_EXPRESSIONS);
			}
		}

		if (configBean.getArgumentFields() != null && configBean.getRegExps() != null
		        && configBean.getArgumentFields().size() != configBean.getRegExps().size()) {
			exception.add(Message.SIZE_SHOULD_MATCH, RegularExpression.FIELDS, RegularExpression.REGULAR_EXPRESSIONS);
		}
	}
}
