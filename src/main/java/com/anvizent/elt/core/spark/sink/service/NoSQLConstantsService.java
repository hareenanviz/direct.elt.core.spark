package com.anvizent.elt.core.spark.sink.service;

import java.io.Serializable;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;

import org.codehaus.janino.ExpressionEvaluator;

import com.anvizent.elt.core.lib.exception.DataCorruptedException;
import com.anvizent.elt.core.lib.exception.DateParseException;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidConfigValueException;
import com.anvizent.elt.core.lib.exception.InvalidSituationException;
import com.anvizent.elt.core.lib.exception.UnsupportedCoerceException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.lib.util.TypeConversionUtil;
import com.anvizent.elt.core.spark.operation.service.ExpressionService;
import com.anvizent.elt.core.spark.sink.config.bean.NoSQLConstantsConfigBean;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class NoSQLConstantsService implements Serializable {

	private static final long serialVersionUID = 1L;

	public static LinkedHashMap<String, Object> getConstants(String externalDataPrefix, LinkedHashMap<String, Object> emptyRow,
	        ArrayList<String> emptyArguments, ZoneOffset timezoneOffset, NoSQLConstantsConfigBean constantsConfigBean,
	        ArrayList<ExpressionEvaluator> expressionEvaluators) throws DataCorruptedException, ValidationViolationException, UnsupportedCoerceException,
	        InvalidSituationException, DateParseException, ImproperValidationException, InvalidConfigValueException {
		LinkedHashMap<String, Object> constantsRow = new LinkedHashMap<>();

		if (constantsConfigBean.getFields() != null && !constantsConfigBean.getFields().isEmpty()) {
			evaluateConstant(externalDataPrefix, emptyRow, emptyArguments, constantsRow, constantsConfigBean.getFields(), expressionEvaluators);
		}

		if (constantsConfigBean.getLiteralFields() != null && !constantsConfigBean.getLiteralFields().isEmpty()) {
			evaluateLiteral(externalDataPrefix, timezoneOffset, constantsRow, constantsConfigBean.getLiteralFields(), constantsConfigBean.getLiteralValues(),
			        constantsConfigBean.getLiteralTypes(), constantsConfigBean.getDateFormats());
		}

		return constantsRow;
	}

	private static void evaluateConstant(String externalDataPrefix, LinkedHashMap<String, Object> emptyRow, ArrayList<String> emptyArguments,
	        LinkedHashMap<String, Object> constantsRow, ArrayList<String> fields, ArrayList<ExpressionEvaluator> expressionEvaluators)
	        throws DataCorruptedException, ValidationViolationException {
		for (int i = 0; i < fields.size(); i++) {
			constantsRow.put(externalDataPrefix == null || externalDataPrefix.isEmpty() ? fields.get(i) : externalDataPrefix + fields.get(i),
			        ExpressionService.evaluateExpression(expressionEvaluators.get(i), emptyArguments, emptyArguments, emptyRow));
		}
	}

	private static void evaluateLiteral(String externalDataPrefix, ZoneOffset timezoneOffset, LinkedHashMap<String, Object> constantsRow,
	        ArrayList<String> literalFields, ArrayList<String> literalValues, ArrayList<Class<?>> literalTypes, ArrayList<String> dateFormats)
	        throws UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException, InvalidConfigValueException {
		for (int i = 0; i < literalFields.size(); i++) {
			constantsRow.put(externalDataPrefix == null || externalDataPrefix.isEmpty() ? literalFields.get(i) : externalDataPrefix + literalFields.get(i),
			        TypeConversionUtil.dataTypeConversion(literalValues.get(i), String.class, literalTypes.get(i), getDateFormat(dateFormats, i), null, null,
			                timezoneOffset));
		}
	}

	private static String getDateFormat(ArrayList<String> dateFormats, int index) {
		return dateFormats == null || dateFormats.isEmpty() ? null : dateFormats.get(index);
	}
}
