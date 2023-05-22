package com.anvizent.elt.core.spark.mapping.validator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.CleansingValidationType;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Mapping.ConditionalReplacementCleansing;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Mapping.ConditionalReplacementCleansing.CustomJavaExpressionCleansing;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Mapping.ConditionalReplacementCleansing.EqualsNotEqualsCleansing;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Mapping.ConditionalReplacementCleansing.MatchesNotMatchesRegexCleansing;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Mapping.ConditionalReplacementCleansing.RangeCleansing;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant.Message;
import com.anvizent.elt.core.spark.mapping.config.bean.ConditionalReplacementCleansingConfigBean;
import com.anvizent.elt.core.spark.util.CollectionUtil;

/**
 * @author Hareen Bejjanki
 *
 */
public class ConditionalReplacementCleansingValidator extends MappingValidator {

	private static final long serialVersionUID = 1L;

	@Override
	public MappingConfigBean validateAndSetBean(LinkedHashMap<String, String> configs) throws ImproperValidationException, InvalidConfigException {

		ConditionalReplacementCleansingConfigBean configBean = new ConditionalReplacementCleansingConfigBean();

		ArrayList<String> fields = ConfigUtil.getArrayList(configs, ConditionalReplacementCleansing.FIELDS, exception);
		ArrayList<String> validationTypes = ConfigUtil.getArrayList(configs, ConditionalReplacementCleansing.VALIDATION_TYPES, exception);
		ArrayList<String> dateFormats = ConfigUtil.getArrayList(configs, ConditionalReplacementCleansing.DATE_FORMATS, exception);
		ArrayList<String> replacementValues = ConfigUtil.getArrayList(configs, ConditionalReplacementCleansing.REPLACEMENT_VALUES, exception);
		ArrayList<String> replacementValuesByFields = ConfigUtil.getArrayList(configs, ConditionalReplacementCleansing.REPLACEMENT_VALUES_BY_FIELDS, exception);

		if (fields == null || fields.isEmpty()) {
			addException(ValidationConstant.Message.SINGLE_KEY_MANDATORY, ConditionalReplacementCleansing.FIELDS);
		} else {

			HashSet<String> uniqueFields = new HashSet<>(fields);
			if (fields.size() != uniqueFields.size()) {
				addException("Only one cleansing validation is supported per field.", ConditionalReplacementCleansing.FIELDS);
			}

			if (validationTypes == null || validationTypes.isEmpty()) {
				addException(ValidationConstant.Message.SINGLE_KEY_MANDATORY, ConditionalReplacementCleansing.VALIDATION_TYPES);
			} else if (fields.size() != validationTypes.size()) {
				addException(ValidationConstant.Message.SIZE_SHOULD_MATCH, ConditionalReplacementCleansing.FIELDS,
				        ConditionalReplacementCleansing.VALIDATION_TYPES);
			} else {
				for (String type : validationTypes) {
					if (CleansingValidationType.getInstance(type) == null) {
						addException(ValidationConstant.Message.INVALID_VALIDATION_TYPE, ConditionalReplacementCleansing.VALIDATION_TYPES);
						break;
					}
				}
			}

			if (dateFormats != null && fields.size() != dateFormats.size()) {
				addException(ValidationConstant.Message.SIZE_SHOULD_MATCH, ConditionalReplacementCleansing.FIELDS,
				        ConditionalReplacementCleansing.DATE_FORMATS);
			}

			if ((replacementValues == null || replacementValues.isEmpty()) && (replacementValuesByFields == null || replacementValuesByFields.isEmpty())) {
				addException(ValidationConstant.Message.EITHER_OF_THE_KEY_IS_MANDATORY, ConditionalReplacementCleansing.REPLACEMENT_VALUES,
				        ConditionalReplacementCleansing.REPLACEMENT_VALUES_BY_FIELDS);
			} else if ((replacementValuesByFields == null || replacementValuesByFields.isEmpty()) && fields.size() != replacementValues.size()) {
				addException(ValidationConstant.Message.SIZE_SHOULD_MATCH, ConditionalReplacementCleansing.FIELDS,
				        ConditionalReplacementCleansing.REPLACEMENT_VALUES);
			} else if ((replacementValues == null || replacementValues.isEmpty()) && fields.size() != replacementValuesByFields.size()) {
				addException(ValidationConstant.Message.SIZE_SHOULD_MATCH, ConditionalReplacementCleansing.FIELDS,
				        ConditionalReplacementCleansing.REPLACEMENT_VALUES_BY_FIELDS);
			} else {
				int i = CollectionUtil.getConflictingValuesIndex(replacementValues, replacementValuesByFields);

				if (i != -1) {
					addException(ValidationConstant.Message.CONFLICTING_VALUES_AT, ConditionalReplacementCleansing.REPLACEMENT_VALUES,
					        ConditionalReplacementCleansing.REPLACEMENT_VALUES_BY_FIELDS, i);
				}
			}
		}

		if (canCreateBean) {
			configBean.setFields(fields);
			configBean.setValidationTypes(CleansingValidationType.getInstances(validationTypes));
			configBean.setDateFormats(dateFormats);
			configBean.setReplacementValues(replacementValues);
			configBean.setReplacementValuesByFields(replacementValuesByFields);

			validateAndSetConfigBean(configBean, configs);
		}

		return configBean;
	}

	private void validateAndSetConfigBean(ConditionalReplacementCleansingConfigBean configBean, LinkedHashMap<String, String> configs)
	        throws ImproperValidationException {

		if (configBean.getValidationTypes().contains(CleansingValidationType.RANGE)) {
			validateRangeAndSetConfigBean(configBean, configs);
		}
		if (configBean.getValidationTypes().contains(CleansingValidationType.EQUAL)) {
			validateEqualsAndSetConfigBean(configBean, configs);
		}
		if (configBean.getValidationTypes().contains(CleansingValidationType.NOT_EQUAL)) {
			validateNotEqualsAndSetConfigBean(configBean, configs);
		}
		if (configBean.getValidationTypes().contains(CleansingValidationType.MATCHES_REGEX)) {
			validateMatchesRegexAndSetConfigBean(configBean, configs);
		}
		if (configBean.getValidationTypes().contains(CleansingValidationType.NOT_MATCHES_REGEX)) {
			validateNotMatchesRegexAndSetConfigBean(configBean, configs);
		}
		if (configBean.getValidationTypes().contains(CleansingValidationType.CUSTOM_EXPRESSION)) {
			validateCustomExpressionAndSetConfigBean(configBean, configs);
		}

		if (canCreateBean) {
			validateConfigValues(configBean);
		}
	}

	private void validateRangeAndSetConfigBean(ConditionalReplacementCleansingConfigBean configBean, LinkedHashMap<String, String> configs)
	        throws ImproperValidationException {
		ArrayList<String> minValues = ConfigUtil.getArrayList(configs, RangeCleansing.MIN, exception);
		ArrayList<String> maxValues = ConfigUtil.getArrayList(configs, RangeCleansing.MAX, exception);

		if (minValues == null || minValues.isEmpty()) {
			addException(ValidationConstant.Message.SINGLE_KEY_MANDATORY, RangeCleansing.MIN);
		} else if (configBean.getFields().size() != minValues.size()) {
			addException(ValidationConstant.Message.SIZE_SHOULD_MATCH, ConditionalReplacementCleansing.FIELDS, RangeCleansing.MIN);
		}

		if (maxValues == null || maxValues.isEmpty()) {
			addException(ValidationConstant.Message.SINGLE_KEY_MANDATORY, RangeCleansing.MAX);
		} else if (configBean.getFields().size() != maxValues.size()) {
			addException(ValidationConstant.Message.SIZE_SHOULD_MATCH, ConditionalReplacementCleansing.FIELDS, RangeCleansing.MAX);
		}

		configBean.setMin(minValues);
		configBean.setMax(maxValues);
	}

	private void validateEqualsAndSetConfigBean(ConditionalReplacementCleansingConfigBean configBean, LinkedHashMap<String, String> configs)
	        throws ImproperValidationException {
		ArrayList<String> equalValues = ConfigUtil.getArrayList(configs, EqualsNotEqualsCleansing.EQUALS, exception);

		if (equalValues == null || equalValues.isEmpty()) {
			addException(ValidationConstant.Message.SINGLE_KEY_MANDATORY, EqualsNotEqualsCleansing.EQUALS);
		} else if (equalValues.size() != configBean.getFields().size()) {
			addException(ValidationConstant.Message.SIZE_SHOULD_MATCH, ConditionalReplacementCleansing.FIELDS, EqualsNotEqualsCleansing.EQUALS);
		}

		configBean.setEquals(equalValues);
	}

	private void validateNotEqualsAndSetConfigBean(ConditionalReplacementCleansingConfigBean configBean, LinkedHashMap<String, String> configs)
	        throws ImproperValidationException {
		ArrayList<String> notEqualValues = ConfigUtil.getArrayList(configs, EqualsNotEqualsCleansing.NOT_EQUALS, exception);

		if (notEqualValues == null || notEqualValues.isEmpty()) {
			addException(ValidationConstant.Message.SINGLE_KEY_MANDATORY, EqualsNotEqualsCleansing.NOT_EQUALS);
		} else if (notEqualValues.size() != configBean.getFields().size()) {
			addException(ValidationConstant.Message.SIZE_SHOULD_MATCH, ConditionalReplacementCleansing.FIELDS, EqualsNotEqualsCleansing.NOT_EQUALS);
		}

		configBean.setNotEquals(notEqualValues);
	}

	private void validateMatchesRegexAndSetConfigBean(ConditionalReplacementCleansingConfigBean configBean, LinkedHashMap<String, String> configs)
	        throws ImproperValidationException {
		ArrayList<String> matchesRegexValues = ConfigUtil.getArrayList(configs, MatchesNotMatchesRegexCleansing.MATCHES_REGEX, exception);

		if (matchesRegexValues == null || matchesRegexValues.isEmpty()) {
			addException(ValidationConstant.Message.SINGLE_KEY_MANDATORY, MatchesNotMatchesRegexCleansing.MATCHES_REGEX);
		} else if (matchesRegexValues.size() != configBean.getFields().size()) {
			addException(ValidationConstant.Message.SIZE_SHOULD_MATCH, ConditionalReplacementCleansing.FIELDS, MatchesNotMatchesRegexCleansing.MATCHES_REGEX);
		}

		configBean.setMatchesRegex(matchesRegexValues);
	}

	private void validateNotMatchesRegexAndSetConfigBean(ConditionalReplacementCleansingConfigBean configBean, LinkedHashMap<String, String> configs)
	        throws ImproperValidationException {
		ArrayList<String> notMatchesRegexValues = ConfigUtil.getArrayList(configs, MatchesNotMatchesRegexCleansing.NOT_MATCHES_REGEX, exception);

		if (notMatchesRegexValues == null || notMatchesRegexValues.isEmpty()) {
			addException(ValidationConstant.Message.SINGLE_KEY_MANDATORY, MatchesNotMatchesRegexCleansing.NOT_MATCHES_REGEX);
		} else if (notMatchesRegexValues.size() != configBean.getFields().size()) {
			addException(ValidationConstant.Message.SIZE_SHOULD_MATCH, ConditionalReplacementCleansing.FIELDS,
			        MatchesNotMatchesRegexCleansing.NOT_MATCHES_REGEX);
		}

		configBean.setNotMatchesRegex(notMatchesRegexValues);
	}

	private void validateCustomExpressionAndSetConfigBean(ConditionalReplacementCleansingConfigBean configBean, LinkedHashMap<String, String> configs)
	        throws ImproperValidationException {
		ArrayList<String> expressions = ConfigUtil.getArrayList(configs, CustomJavaExpressionCleansing.EXPRESSIONS, exception);
		ArrayList<String> argumentFields = ConfigUtil.getArrayList(configs, CustomJavaExpressionCleansing.ARGUMENT_FIELDS, exception);
		ArrayList<Class<?>> argumentTypes = ConfigUtil.getArrayListOfClass(configs, CustomJavaExpressionCleansing.ARGUMENT_TYPES, exception);

		if (expressions == null || expressions.isEmpty()) {
			addException(ValidationConstant.Message.SINGLE_KEY_MANDATORY, CustomJavaExpressionCleansing.EXPRESSIONS);
		} else if (expressions.size() != configBean.getFields().size()) {
			addException(ValidationConstant.Message.SIZE_SHOULD_MATCH, ConditionalReplacementCleansing.FIELDS, CustomJavaExpressionCleansing.EXPRESSIONS);
		}

		if (argumentFields == null || argumentFields.isEmpty()) {
			addException(ValidationConstant.Message.SINGLE_KEY_MANDATORY, CustomJavaExpressionCleansing.ARGUMENT_FIELDS);
		}
		if (argumentTypes == null || argumentTypes.isEmpty()) {
			addException(ValidationConstant.Message.SINGLE_KEY_MANDATORY, CustomJavaExpressionCleansing.ARGUMENT_TYPES);
		}
		if (argumentFields != null && argumentTypes != null && argumentFields.size() != argumentTypes.size()) {
			addException(ValidationConstant.Message.SIZE_SHOULD_MATCH, CustomJavaExpressionCleansing.ARGUMENT_FIELDS,
			        CustomJavaExpressionCleansing.ARGUMENT_TYPES);
		}

		configBean.setExpressions(expressions);
		configBean.setArgumentFields(argumentFields);
		configBean.setArgumentTypes(argumentTypes);
	}

	private void validateConfigValues(ConditionalReplacementCleansingConfigBean configBean) {
		for (int i = 0; i < configBean.getValidationTypes().size(); i++) {
			CleansingValidationType cleansingType = configBean.getValidationTypes().get(i);

			if (cleansingType.equals(CleansingValidationType.RANGE)) {
				validateMinMaxValues(configBean.getMin().get(i), configBean.getMax().get(i));
			} else if (cleansingType.equals(CleansingValidationType.EQUAL)) {
				validateGeneralValues(configBean.getEquals().get(i), EqualsNotEqualsCleansing.EQUALS);
			} else if (cleansingType.equals(CleansingValidationType.NOT_EQUAL)) {
				validateGeneralValues(configBean.getNotEquals().get(i), EqualsNotEqualsCleansing.NOT_EQUALS);
			} else if (cleansingType.equals(CleansingValidationType.MATCHES_REGEX)) {
				validateGeneralValues(configBean.getMatchesRegex().get(i), MatchesNotMatchesRegexCleansing.MATCHES_REGEX);
			} else if (cleansingType.equals(CleansingValidationType.NOT_MATCHES_REGEX)) {
				validateGeneralValues(configBean.getNotMatchesRegex().get(i), MatchesNotMatchesRegexCleansing.NOT_MATCHES_REGEX);
			} else if (cleansingType.equals(CleansingValidationType.CUSTOM_EXPRESSION)) {
				validateGeneralValues(configBean.getExpressions().get(i), CustomJavaExpressionCleansing.EXPRESSIONS);
			}

			if (!canCreateBean) {
				break;
			}
		}
	}

	private void validateMinMaxValues(String min, String max) {
		if (isAnyEmpty(min, max)) {
			addException(Message.CAN_NOT_BE_EMPTY_2, RangeCleansing.MIN, RangeCleansing.MAX);
		}
	}

	private void validateGeneralValues(String value, String key) {
		if (isAnyEmpty(value)) {
			addException(Message.LIST_CAN_NOT_HAVE_EMPTY, key);
		}
	}

	private boolean isAnyEmpty(String... values) {
		boolean isAnyEmpty = false;

		for (String value : values) {
			if (value == null || value.isEmpty()) {
				isAnyEmpty = true;
				break;
			}
		}

		return isAnyEmpty;
	}

	@Override
	public String[] getConfigsList() throws ImproperValidationException {
		return new String[] { ConditionalReplacementCleansing.FIELDS, ConditionalReplacementCleansing.VALIDATION_TYPES,
		        ConditionalReplacementCleansing.DATE_FORMATS, ConditionalReplacementCleansing.REPLACEMENT_VALUES, RangeCleansing.MIN, RangeCleansing.MAX,
		        EqualsNotEqualsCleansing.EQUALS, EqualsNotEqualsCleansing.NOT_EQUALS };
	}

}
