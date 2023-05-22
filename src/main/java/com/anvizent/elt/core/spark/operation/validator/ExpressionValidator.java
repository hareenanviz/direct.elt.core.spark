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
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.Expression;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.operation.config.bean.ExpressionConfigBean;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public class ExpressionValidator extends Validator {

	private static final long serialVersionUID = 1L;

	public ExpressionValidator(Factory factory) {
		super(factory);
	}

	@Override
	public ConfigBean validateFactoryConfig(LinkedHashMap<String, String> configs, ConfigAndMappingConfigBeans configAndMappingConfigBeans)
	        throws InvalidConfigException, ImproperValidationException, UnsupportedException {
		validateMandatoryFields(configs, Operation.General.EXPRESSIONS, Expression.EXPRESSIONS_FIELD_NAMES, Operation.General.ARGUMENT_FIELDS,
		        Expression.RETURN_TYPES, Operation.General.ARGUMENT_TYPES);

		ExpressionConfigBean configBean = new ExpressionConfigBean();
		setConfigBean(configBean, configs);
		validateConfigBean(configBean, configs);
		return configBean;
	}

	private void setConfigBean(ExpressionConfigBean configBean, LinkedHashMap<String, String> configs)
	        throws ImproperValidationException, InvalidConfigException {
		configBean.setExpressions(ConfigUtil.getArrayList(configs, Operation.General.EXPRESSIONS, exception));
		configBean.setExpressionsFieldNames(ConfigUtil.getArrayList(configs, Operation.Expression.EXPRESSIONS_FIELD_NAMES, exception));
		configBean.setExpressionsFieldIndexes(ConfigUtil.getArrayListOfIntegers(configs, Operation.Expression.EXPRESSIONS_FIELD_NAMES_INDEXES, exception));
		configBean.setArgumentFields(ConfigUtil.getArrayList(configs, Operation.General.ARGUMENT_FIELDS, exception));
		configBean.setArgumentTypes(ConfigUtil.getArrayListOfClass(configs, Operation.General.ARGUMENT_TYPES, exception));
		configBean.setReturnTypes(ConfigUtil.getArrayListOfClass(configs, Operation.Expression.RETURN_TYPES, exception));
		configBean.setPrecisions(ConfigUtil.getArrayListOfIntegers(configs, General.DECIMAL_PRECISIONS, exception));
		configBean.setScales(ConfigUtil.getArrayListOfIntegers(configs, General.DECIMAL_SCALES, exception));
	}

	private void validateConfigBean(ExpressionConfigBean configBean, LinkedHashMap<String, String> configs) throws ImproperValidationException {
		if (configBean.getExpressions() != null && !configBean.getExpressions().isEmpty()) {
			if (configBean.getReturnTypes() != null && !configBean.getReturnTypes().isEmpty()
			        && configBean.getExpressions().size() != configBean.getReturnTypes().size()) {
				exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, Operation.General.EXPRESSIONS, Operation.Expression.RETURN_TYPES);
			}
		}

		if (configBean.getExpressionsFieldNames() != null && !configBean.getExpressionsFieldNames().isEmpty()) {
			if (configBean.getExpressionsFieldIndexes() != null && !configBean.getExpressionsFieldIndexes().isEmpty()
			        && configBean.getExpressionsFieldIndexes().size() != configBean.getExpressionsFieldNames().size()) {
				exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, Operation.Expression.EXPRESSIONS_FIELD_NAMES,
				        Operation.Expression.EXPRESSIONS_FIELD_NAMES_INDEXES);
			}
			if (configBean.getExpressions() != null && !configBean.getExpressions().isEmpty()
			        && configBean.getExpressions().size() != configBean.getExpressionsFieldNames().size()) {
				exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, Operation.General.EXPRESSIONS, Operation.Expression.EXPRESSIONS_FIELD_NAMES);
			}
		}

		if ((configBean.getArgumentFields() != null && !configBean.getArgumentFields().isEmpty())
		        && (configBean.getArgumentTypes() != null && !configBean.getArgumentTypes().isEmpty())) {
			if (configBean.getArgumentFields().size() != configBean.getArgumentTypes().size()) {
				exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, Operation.General.ARGUMENT_FIELDS, Operation.General.ARGUMENT_TYPES);
			}
		}
	}
}
