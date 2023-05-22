package com.anvizent.elt.core.spark.mapping.service;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.regex.Pattern;

import org.apache.commons.lang3.Range;
import org.codehaus.janino.ExpressionEvaluator;

import com.anvizent.elt.core.lib.exception.DataCorruptedException;
import com.anvizent.elt.core.lib.exception.DateParseException;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidConfigValueException;
import com.anvizent.elt.core.lib.exception.InvalidSituationException;
import com.anvizent.elt.core.lib.exception.UnsupportedCoerceException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.lib.util.TypeConversionUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Mapping.ConditionalReplacementCleansing;
import com.anvizent.elt.core.spark.exception.CleansingValidationException;
import com.anvizent.elt.core.spark.operation.service.ExpressionService;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class CleansingService {

	@SuppressWarnings("rawtypes")
	public static void rangeValidationCleansing(HashMap<String, Object> newRow, String key, Object fieldValue, String min, String max, String dateFormat,
	        Object replacementValue, Class fieldType)
	        throws DateParseException, CleansingValidationException, UnsupportedCoerceException, ImproperValidationException, InvalidConfigValueException {

		if (fieldValue == null) {
			newRow.put(key, fieldValue);
		} else if (fieldType.equals(Byte.class)) {
			Range<Byte> range = Range.between(Byte.valueOf(min), Byte.valueOf(max));
			if (range.contains(Byte.valueOf(fieldValue.toString()))) {
				newRow.put(key, replacementValue);
			}
		} else if (fieldType.equals(Short.class)) {
			Range<Short> range = Range.between(Short.valueOf(min), Short.valueOf(max));
			if (range.contains(Short.valueOf(fieldValue.toString()))) {
				newRow.put(key, replacementValue);
			}
		} else if (fieldType.equals(Character.class)) {
			if (min.length() == 1 && max.length() == 1 && fieldValue.toString().length() == 1) {
				Range<Character> range = Range.between((Character) min.charAt(0), (Character) max.charAt(0));
				if (range.contains(fieldValue.toString().charAt(0))) {
					newRow.put(key, replacementValue);
				}
			} else {
				throw new CleansingValidationException("Unsupported field value: " + fieldValue + " for field type: " + fieldType.getName());
			}
		} else if (fieldType.equals(Integer.class)) {
			Range<Integer> range = Range.between(Integer.valueOf(min), Integer.valueOf(max));
			if (range.contains(Integer.valueOf(fieldValue.toString()))) {
				newRow.put(key, replacementValue);
			}
		} else if (fieldType.equals(Long.class)) {
			Range<Long> range = Range.between(Long.valueOf(min), Long.valueOf(max));
			if (range.contains(Long.valueOf(fieldValue.toString()))) {
				newRow.put(key, replacementValue);
			}
		} else if (fieldType.equals(Float.class)) {
			Range<Float> range = Range.between(Float.valueOf(min), Float.valueOf(max));
			if (range.contains(Float.valueOf(fieldValue.toString()))) {
				newRow.put(key, replacementValue);
			}
		} else if (fieldType.equals(Double.class)) {
			Range<Double> range = Range.between(Double.valueOf(min), Double.valueOf(max));
			if (range.contains(Double.valueOf(fieldValue.toString()))) {
				newRow.put(key, replacementValue);
			}
		} else if (fieldType.equals(String.class)) {
			Range<String> range = Range.between(min, max);
			if (range.contains(fieldValue.toString())) {
				newRow.put(key, replacementValue);
			}
		} else if (fieldType.equals(BigDecimal.class)) {
			Range<BigDecimal> range = Range.between(new BigDecimal(min), new BigDecimal(max));
			if (range.contains(new BigDecimal(fieldValue.toString()))) {
				newRow.put(key, replacementValue);
			}
		} else if (fieldType.equals(Date.class)) {
			Date minValue = TypeConversionUtil.stringToDateConversion(min, fieldType, dateFormat, ConditionalReplacementCleansing.DATE_FORMATS);
			Date maxValue = TypeConversionUtil.stringToDateConversion(max, fieldType, dateFormat, ConditionalReplacementCleansing.DATE_FORMATS);

			Range<Date> range = Range.between(minValue, maxValue);
			if (range.contains((Date) fieldValue)) {
				newRow.put(key, replacementValue);
			}
		} else {
			throw new CleansingValidationException("Range validation cleansing not supported for field type: " + fieldType.getName());
		}

	}

	@SuppressWarnings("rawtypes")
	public static void equalsCleansing(HashMap<String, Object> newRow, String key, Object fieldValue, String equalToValue, String dateFormat,
	        Object replacementValue, Class fieldType)
	        throws UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException, InvalidConfigValueException {

		if (fieldValue == null) {
			newRow.put(key, fieldValue);
		} else if (fieldType.equals(Date.class)) {
			if (fieldValue
			        .equals(TypeConversionUtil.stringToDateConversion(equalToValue, fieldType, dateFormat, ConditionalReplacementCleansing.DATE_FORMATS))) {
				newRow.put(key, replacementValue);
			}
		} else {
			if (fieldValue.equals(
			        TypeConversionUtil.stringToOtherTypeConversion(equalToValue, fieldType, dateFormat, null, ConditionalReplacementCleansing.DATE_FORMATS))) {
				newRow.put(key, replacementValue);
			}
		}
	}

	@SuppressWarnings("rawtypes")
	public static void notEqualsCleansing(HashMap<String, Object> newRow, String key, Object fieldValue, String notEqualToValue, String dateFormat,
	        Object replacementValue, Class fieldType)
	        throws UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException, InvalidConfigValueException {

		if (fieldValue == null) {
			newRow.put(key, fieldValue);
		} else if (fieldType.equals(Date.class)) {
			if (!fieldValue
			        .equals(TypeConversionUtil.stringToDateConversion(notEqualToValue, fieldType, dateFormat, ConditionalReplacementCleansing.DATE_FORMATS))) {
				newRow.put(key, replacementValue);
			}
		} else {
			if (!fieldValue.equals(TypeConversionUtil.stringToOtherTypeConversion(notEqualToValue, fieldType, dateFormat, null,
			        ConditionalReplacementCleansing.DATE_FORMATS))) {
				newRow.put(key, replacementValue);
			}
		}
	}

	@SuppressWarnings("rawtypes")
	public static void emptyCleansing(HashMap<String, Object> newRow, String key, Object fieldValue, Object replacementValue, Class javaType) {
		if (javaType.equals(String.class)) {
			if (fieldValue == null || fieldValue.toString().isEmpty()) {
				newRow.put(key, replacementValue);
			}
		} else {
			if (fieldValue == null) {
				newRow.put(key, replacementValue);
			}
		}
	}

	@SuppressWarnings("rawtypes")
	public static void notEmptyCleansing(HashMap<String, Object> newRow, String key, Object fieldValue, Object replacementValue, Class javaType) {
		if (javaType.equals(String.class)) {
			if (fieldValue != null && !fieldValue.toString().isEmpty()) {
				newRow.put(key, replacementValue);
			}
		} else {
			if (fieldValue != null) {
				newRow.put(key, replacementValue);
			}
		}
	}

	public static void matchesRegexCleansing(HashMap<String, Object> newRow, String key, Object fieldValue, Object replacementValue, String regex) {
		if (fieldValue == null) {
			newRow.put(key, fieldValue);
		} else {
			if (Pattern.matches(regex, fieldValue.toString())) {
				newRow.put(key, replacementValue);
			}
		}
	}

	public static void notMatchesRegexCleansing(HashMap<String, Object> newRow, String key, Object fieldValue, Object replacementValue, String regex) {
		if (fieldValue == null) {
			newRow.put(key, fieldValue);
		} else {
			if (!Pattern.matches(regex, fieldValue.toString())) {
				newRow.put(key, replacementValue);
			}
		}
	}

	public static void customJavaExpressionCleansing(ExpressionEvaluator expressionEvaluator, ArrayList<String> argumentFieldsByExpression,
	        ArrayList<String> argumentFieldAliases, HashMap<String, Object> row, HashMap<String, Object> newRow, String key, Object replacementValue)
	        throws DataCorruptedException, ValidationViolationException {
		Object evaluateExpression = ExpressionService.evaluateExpression(expressionEvaluator, argumentFieldsByExpression, argumentFieldAliases, newRow);

		if (evaluateExpression.equals(Boolean.TRUE)) {
			newRow.put(key, replacementValue);
		}
	}
}
