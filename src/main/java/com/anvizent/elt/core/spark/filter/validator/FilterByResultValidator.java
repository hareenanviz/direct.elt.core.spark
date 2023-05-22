package com.anvizent.elt.core.spark.filter.validator;

import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Filter.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.ResultFetcher;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant.Message;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.filter.config.bean.FilterByResultConfigBean;
import com.anvizent.elt.core.spark.filter.service.FilterService;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class FilterByResultValidator extends Validator {

	private static final long serialVersionUID = 1L;

	public FilterByResultValidator(Factory factory) {
		super(factory);
	}

	@Override
	public ConfigBean validateFactoryConfig(LinkedHashMap<String, String> configs, ConfigAndMappingConfigBeans configAndMappingConfigBeans)
	        throws InvalidConfigException, ImproperValidationException, UnsupportedException {
		validateMandatoryFields(configs, ResultFetcher.CLASS_NAMES, ResultFetcher.METHOD_NAMES);

		FilterByResultConfigBean configBean = new FilterByResultConfigBean();

		setConfigBean(configBean, configs);
		validateConfigBean(configBean);

		return configBean;
	}

	private void setConfigBean(FilterByResultConfigBean configBean, LinkedHashMap<String, String> configs) throws ImproperValidationException {
		FilterService.setEmitStreams(configBean, configs, exception);
		configBean.setClassNames(ConfigUtil.getArrayList(configs, ResultFetcher.CLASS_NAMES, exception));
		configBean.setMethodNames(ConfigUtil.getArrayList(configs, ResultFetcher.METHOD_NAMES, exception));
		configBean.setMethodArgumentFields(ConfigUtil.getArrayListOfArrayList(configs, ResultFetcher.METHOD_ARGUMENT_FIELDS, exception));
	}

	private void validateConfigBean(FilterByResultConfigBean configBean) throws ImproperValidationException {
		if (configBean.getClassNames() != null && !configBean.getClassNames().isEmpty()) {

			if (configBean.getEmitStreamNames().size() < configBean.getClassNames().size()
			        || (configBean.getEmitStreamNames().size() - configBean.getClassNames().size() > 1)) {
				exception.add(Message.COUNT_EQUAL_OR_PLUS_1, General.EMIT_STREAM_NAMES, ResultFetcher.CLASS_NAMES);
			}

			if (configBean.getMethodNames() != null && !configBean.getMethodNames().isEmpty()
			        && configBean.getMethodNames().size() != configBean.getClassNames().size()) {
				exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, ResultFetcher.METHOD_NAMES, ResultFetcher.CLASS_NAMES);
			}

			if (configBean.getMethodArgumentFields() != null && !configBean.getMethodArgumentFields().isEmpty()
			        && configBean.getMethodArgumentFields().size() != configBean.getClassNames().size()) {
				exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, ResultFetcher.METHOD_ARGUMENT_FIELDS, ResultFetcher.CLASS_NAMES);
			}
		}
	}
}
