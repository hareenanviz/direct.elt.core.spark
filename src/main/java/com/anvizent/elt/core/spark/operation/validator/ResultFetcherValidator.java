package com.anvizent.elt.core.spark.operation.validator;

import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.ResultFetcher;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.operation.config.bean.ResultFetcherConfigBean;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public class ResultFetcherValidator extends Validator {

	private static final long serialVersionUID = 1L;

	public ResultFetcherValidator(Factory factory) {
		super(factory);
	}

	@Override
	public ConfigBean validateFactoryConfig(LinkedHashMap<String, String> configs, ConfigAndMappingConfigBeans configAndMappingConfigBeans)
	        throws InvalidConfigException, ImproperValidationException, UnsupportedException {

		validateMandatoryFields(configs, ResultFetcher.CLASS_NAMES, ResultFetcher.METHOD_NAMES, ResultFetcher.RETURN_FIELDS);

		ResultFetcherConfigBean configBean = new ResultFetcherConfigBean();

		setConfigBean(configBean, configs);
		validateConfigBean(configBean, configs);

		return configBean;
	}

	private void setConfigBean(ResultFetcherConfigBean configBean, LinkedHashMap<String, String> configs)
	        throws ImproperValidationException, InvalidConfigException {
		configBean.setClassNames(ConfigUtil.getArrayList(configs, ResultFetcher.CLASS_NAMES, exception));
		configBean.setMethodNames(ConfigUtil.getArrayList(configs, ResultFetcher.METHOD_NAMES, exception));
		configBean.setVarArgsIndexes(ConfigUtil.getArrayListOfIntegers(configs, ResultFetcher.VAR_ARGS_INDEXES, exception));
		configBean.setMethodArgumentFields(ConfigUtil.getArrayListOfArrayList(configs, ResultFetcher.METHOD_ARGUMENT_FIELDS, exception));
		configBean.setReturnFields(ConfigUtil.getArrayList(configs, ResultFetcher.RETURN_FIELDS, exception));
		configBean.setReturnFieldsIndexes(ConfigUtil.getArrayListOfIntegers(configs, ResultFetcher.RETURN_FIELDS_INDEXES, exception));
	}

	private void validateConfigBean(ResultFetcherConfigBean configBean, LinkedHashMap<String, String> configs) throws ImproperValidationException {
		if (configBean.getClassNames() != null && !configBean.getClassNames().isEmpty()) {
			if (configBean.getMethodNames() != null && !configBean.getMethodNames().isEmpty()
			        && configBean.getMethodNames().size() != configBean.getClassNames().size()) {
				exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, ResultFetcher.METHOD_NAMES, ResultFetcher.CLASS_NAMES);
			}

			if (configBean.getVarArgsIndexes() != null && !configBean.getVarArgsIndexes().isEmpty()
			        && configBean.getVarArgsIndexes().size() != configBean.getClassNames().size()) {
				exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, ResultFetcher.VAR_ARGS_INDEXES, ResultFetcher.CLASS_NAMES);
			}

			if (configBean.getReturnFields() != null && !configBean.getReturnFields().isEmpty()
			        && configBean.getReturnFields().size() != configBean.getClassNames().size()) {
				exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, ResultFetcher.RETURN_FIELDS, ResultFetcher.CLASS_NAMES);
			}

			if (configBean.getReturnFieldsIndexes() != null && !configBean.getReturnFieldsIndexes().isEmpty()
			        && configBean.getReturnFieldsIndexes().size() != configBean.getClassNames().size()) {
				exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, ResultFetcher.RETURN_FIELDS_INDEXES, ResultFetcher.CLASS_NAMES);
			}

			if (configBean.getMethodArgumentFields() != null && !configBean.getMethodArgumentFields().isEmpty()
			        && configBean.getMethodArgumentFields().size() != configBean.getClassNames().size()) {
				exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, ResultFetcher.METHOD_ARGUMENT_FIELDS, ResultFetcher.CLASS_NAMES);
			}
		}
	}
}
