package com.anvizent.elt.core.spark.mapping.function;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ExpressionEvaluator;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;
import com.anvizent.elt.core.lib.exception.DataCorruptedException;
import com.anvizent.elt.core.lib.exception.DateParseException;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.InvalidConfigValueException;
import com.anvizent.elt.core.lib.exception.InvalidSituationException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.UnsupportedCoerceException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.lib.function.AnvizentFunction;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.lib.function.MappingFunction;
import com.anvizent.elt.core.lib.util.TypeConversionUtil;
import com.anvizent.elt.core.spark.constant.CleansingValidationType;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Mapping.ConditionalReplacementCleansing;
import com.anvizent.elt.core.spark.exception.CleansingValidationException;
import com.anvizent.elt.core.spark.mapping.config.bean.ConditionalReplacementCleansingConfigBean;
import com.anvizent.elt.core.spark.mapping.service.CleansingService;
import com.anvizent.elt.core.spark.operation.service.ExpressionService;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ConditionalReplacementCleansingMappingFunction extends AnvizentFunction implements MappingFunction {

	private static final long serialVersionUID = 1L;

	private List<ExpressionEvaluator> expressionEvaluators;
	private List<ArrayList<String>> argumentFieldsByExpression;
	private List<ArrayList<String>> argumentFieldAliases;

	public ConditionalReplacementCleansingMappingFunction(ConfigBean configBean, MappingConfigBean mappingConfigBean,
	        LinkedHashMap<String, AnvizentDataType> structure, LinkedHashMap<String, AnvizentDataType> newStructure,
	        ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails)
	        throws InvalidArgumentsException {
		super(configBean, mappingConfigBean, structure, newStructure, null, accumulators, errorHandlerSinkFunction, jobDetails);
	}

	@Override
	public HashMap<String, Object> process(HashMap<String, Object> row) throws ValidationViolationException, RecordProcessingException, DataCorruptedException {
		initExpressionEvaluators();

		try {
			HashMap<String, Object> newRow = new HashMap<>(row);

			cleansing(newRow, row);

			return newRow;
		} catch (UnsupportedCoerceException | InvalidSituationException | CleansingValidationException exception) {
			throw new DataCorruptedException(exception.getMessage(), exception);
		} catch (InvalidConfigValueException | DateParseException exception) {
			throw new ValidationViolationException(exception.getMessage(), exception);
		}
	}

	private void cleansing(HashMap<String, Object> newRow, HashMap<String, Object> row)
	        throws DataCorruptedException, UnsupportedCoerceException, InvalidSituationException, CleansingValidationException, DateParseException,
	        ImproperValidationException, ValidationViolationException, InvalidConfigValueException {
		ConditionalReplacementCleansingConfigBean configBean = (ConditionalReplacementCleansingConfigBean) mappingConfigBean;

		for (int i = 0; i < configBean.getFields().size(); i++) {
			if (row.containsKey(configBean.getFields().get(i))) {
				int index = configBean.getFields().indexOf(configBean.getFields().get(i));
				String dateFormat = configBean.getDateFormats() == null || configBean.getDateFormats().isEmpty() ? null
				        : configBean.getDateFormats().get(index);

				Object replacementValue = getReplacementValue(configBean.getFields().get(i), index, dateFormat, row);
				cleansing(row, newRow, configBean.getFields().get(i), row.get(configBean.getFields().get(i)), dateFormat, replacementValue, index);
			}
		}
	}

	private void initExpressionEvaluators() throws ValidationViolationException {
		ConditionalReplacementCleansingConfigBean configBean = (ConditionalReplacementCleansingConfigBean) mappingConfigBean;

		if (expressionEvaluators == null) {
			expressionEvaluators = new ArrayList<>();
			argumentFieldsByExpression = new ArrayList<>();
			argumentFieldAliases = new ArrayList<>();
		}

		if (expressionEvaluators.isEmpty()) {
			for (int i = 0; i < configBean.getFields().size(); i++) {
				add(configBean, i);
			}
		}
	}

	private void add(ConditionalReplacementCleansingConfigBean configBean, int index) throws ValidationViolationException {
		CleansingValidationType cleansingValidationType = configBean.getValidationTypes().get(index);

		if (!cleansingValidationType.equals(CleansingValidationType.RANGE) && !cleansingValidationType.equals(CleansingValidationType.EQUAL)
		        && !cleansingValidationType.equals(CleansingValidationType.NOT_EQUAL) && !cleansingValidationType.equals(CleansingValidationType.EMPTY)
		        && !cleansingValidationType.equals(CleansingValidationType.NOT_EMPTY) && !cleansingValidationType.equals(CleansingValidationType.MATCHES_REGEX)
		        && !cleansingValidationType.equals(CleansingValidationType.NOT_MATCHES_REGEX)) {
			ArrayList<String> argumentFieldAliases = new ArrayList<>();
			ArrayList<String> argumentFieldsByExpression = new ArrayList<>();
			ArrayList<Class<?>> argumentTypesByExpression = new ArrayList<>();

			String decodedExpression = ExpressionService.decodeExpression(configBean.getExpressions().get(index), argumentFieldsByExpression,
			        argumentTypesByExpression, configBean.getArgumentTypes(), argumentFieldAliases, configBean.getArgumentFields());

			try {
				this.argumentFieldsByExpression.add(argumentFieldsByExpression);
				this.argumentFieldAliases.add(argumentFieldAliases);
				expressionEvaluators.add(
				        getExpressionEvaluator(decodedExpression, Boolean.class, argumentFieldsByExpression, argumentTypesByExpression, argumentFieldAliases));
			} catch (CompileException e) {
				throw new ValidationViolationException("Invalid expression, details: ", e);
			}
		} else {
			expressionEvaluators.add(null);
			this.argumentFieldsByExpression.add(null);
			this.argumentFieldAliases.add(null);
		}
	}

	private ExpressionEvaluator getExpressionEvaluator(String expression, Class<?> returnType, ArrayList<String> argumentFieldsByExpression,
	        ArrayList<Class<?>> argumentTypesByExpression, ArrayList<String> argumentFieldAliases) throws CompileException {
		return new ExpressionEvaluator(expression, returnType, argumentFieldsByExpression.toArray(new String[argumentFieldsByExpression.size()]),
		        argumentTypesByExpression.toArray(new Class[argumentTypesByExpression.size()]), new Class[] { Exception.class },
		        Exception.class.getClassLoader());
	}

	private void cleansing(HashMap<String, Object> row, HashMap<String, Object> newRow, String key, Object fieldValue, String dateFormat,
	        Object replacementValue, int index) throws UnsupportedCoerceException, InvalidSituationException, CleansingValidationException, DateParseException,
	        ImproperValidationException, DataCorruptedException, ValidationViolationException, InvalidConfigValueException {

		ConditionalReplacementCleansingConfigBean configBean = (ConditionalReplacementCleansingConfigBean) mappingConfigBean;

		CleansingValidationType cleansingValidationType = configBean.getValidationTypes().get(index);

		if (cleansingValidationType.equals(CleansingValidationType.RANGE)) {
			rangeValidationCleansing(newRow, index, key, fieldValue, dateFormat, replacementValue);
		} else if (cleansingValidationType.equals(CleansingValidationType.EQUAL)) {
			equalsCleansing(newRow, index, key, fieldValue, dateFormat, replacementValue);
		} else if (cleansingValidationType.equals(CleansingValidationType.NOT_EQUAL)) {
			notEqualsCleansing(newRow, index, key, fieldValue, dateFormat, replacementValue);
		} else if (cleansingValidationType.equals(CleansingValidationType.EMPTY)) {
			emptyCleansing(newRow, index, key, fieldValue, replacementValue);
		} else if (cleansingValidationType.equals(CleansingValidationType.NOT_EMPTY)) {
			notEmptyCleansing(newRow, index, key, fieldValue, replacementValue);
		} else if (cleansingValidationType.equals(CleansingValidationType.MATCHES_REGEX)) {
			matchesRegexCleansing(newRow, index, key, fieldValue, replacementValue);
		} else if (cleansingValidationType.equals(CleansingValidationType.NOT_MATCHES_REGEX)) {
			notMatchesRegexCleansing(newRow, index, key, fieldValue, replacementValue);
		} else {
			customJavaExpressionCleansing(row, newRow, index, key, fieldValue, replacementValue);
		}
	}

	private void rangeValidationCleansing(HashMap<String, Object> newRow, int index, String key, Object fieldValue, String dateFormat, Object replacementValue)
	        throws CleansingValidationException, DateParseException, UnsupportedCoerceException, InvalidSituationException, ImproperValidationException,
	        InvalidConfigValueException {

		ConditionalReplacementCleansingConfigBean configBean = (ConditionalReplacementCleansingConfigBean) mappingConfigBean;

		String min = configBean.getMin().get(index);
		String max = configBean.getMax().get(index);

		CleansingService.rangeValidationCleansing(newRow, key, fieldValue, min, max, dateFormat, replacementValue, structure.get(key).getJavaType());
	}

	private void equalsCleansing(HashMap<String, Object> newRow, int index, String key, Object fieldValue, String dateFormat, Object replacementValue)
	        throws UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException, InvalidConfigValueException {
		String equalToValue = ((ConditionalReplacementCleansingConfigBean) mappingConfigBean).getEquals().get(index);
		CleansingService.equalsCleansing(newRow, key, fieldValue, equalToValue, dateFormat, replacementValue, structure.get(key).getJavaType());
	}

	private void notEqualsCleansing(HashMap<String, Object> newRow, int index, String key, Object fieldValue, String dateFormat, Object replacementValue)
	        throws UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException, InvalidConfigValueException {
		String notEqualToValue = ((ConditionalReplacementCleansingConfigBean) mappingConfigBean).getNotEquals().get(index);
		CleansingService.notEqualsCleansing(newRow, key, fieldValue, notEqualToValue, dateFormat, replacementValue, structure.get(key).getJavaType());
	}

	private void emptyCleansing(HashMap<String, Object> newRow, int index, String key, Object fieldValue, Object replacementValue) {
		CleansingService.emptyCleansing(newRow, key, fieldValue, replacementValue, structure.get(key).getJavaType());
	}

	private void notEmptyCleansing(HashMap<String, Object> newRow, int index, String key, Object fieldValue, Object replacementValue) {
		CleansingService.notEmptyCleansing(newRow, key, fieldValue, replacementValue, structure.get(key).getJavaType());
	}

	private void matchesRegexCleansing(HashMap<String, Object> newRow, int index, String key, Object fieldValue, Object replacementValue) {
		String regex = ((ConditionalReplacementCleansingConfigBean) mappingConfigBean).getMatchesRegex().get(index);
		CleansingService.matchesRegexCleansing(newRow, key, fieldValue, replacementValue, regex);
	}

	private void notMatchesRegexCleansing(HashMap<String, Object> newRow, int index, String key, Object fieldValue, Object replacementValue) {
		String regex = ((ConditionalReplacementCleansingConfigBean) mappingConfigBean).getNotMatchesRegex().get(index);
		CleansingService.notMatchesRegexCleansing(newRow, key, fieldValue, replacementValue, regex);
	}

	private void customJavaExpressionCleansing(HashMap<String, Object> row, HashMap<String, Object> newRow, int index, String key, Object fieldValue,
	        Object replacementValue) throws DataCorruptedException, ValidationViolationException {
		if (row.get(key) != null) {
			if (expressionEvaluators.get(index) == null || argumentFieldsByExpression.get(index) == null || argumentFieldAliases.get(index) == null) {
				new ValidationViolationException("expression evolutor not found for index: " + index + ", column: " + key);
			} else {
				CleansingService.customJavaExpressionCleansing(expressionEvaluators.get(index), argumentFieldsByExpression.get(index),
				        argumentFieldAliases.get(index), row, newRow, key, replacementValue);
			}
		}
	}

	private Object getReplacementValue(String key, int index, String dateFormat, HashMap<String, Object> row)
	        throws UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException, InvalidConfigValueException {
		ConditionalReplacementCleansingConfigBean configBean = (ConditionalReplacementCleansingConfigBean) mappingConfigBean;

		if (configBean.getReplacementValuesByFields() != null && configBean.getReplacementValuesByFields().get(index) != null
		        && !configBean.getReplacementValuesByFields().get(index).isEmpty()) {
			return row.get(configBean.getReplacementValuesByFields().get(index));
		} else if (structure.get(key).getJavaType().equals(Date.class)) {
			return TypeConversionUtil.stringToDateConversion(configBean.getReplacementValues().get(index), structure.get(key).getJavaType(), dateFormat,
			        ConditionalReplacementCleansing.DATE_FORMATS);
		} else {
			return TypeConversionUtil.stringToOtherTypeConversion(configBean.getReplacementValues().get(index), structure.get(key).getJavaType(), dateFormat,
			        null, ConditionalReplacementCleansing.DATE_FORMATS);
		}
	}

}
