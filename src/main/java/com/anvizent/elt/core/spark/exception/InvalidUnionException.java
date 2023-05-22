package com.anvizent.elt.core.spark.exception;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import org.apache.spark.sql.Row;

import com.anvizent.elt.core.lib.exception.ValidationViolationException;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class InvalidUnionException extends ValidationViolationException {

	private static final long serialVersionUID = 1L;

	public InvalidUnionException() {
		super();
	}

	public InvalidUnionException(LinkedHashMap<String, Object> row, LinkedHashMap<String, Object> violatedValues, String message, ArrayList<String> keyFields) {
		super(row, violatedValues, message, keyFields);
	}

	public InvalidUnionException(Row row, LinkedHashMap<String, Object> violatedValues, String message, ArrayList<String> keyFields) {
		super(row, violatedValues, message, keyFields);
	}

	public InvalidUnionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public InvalidUnionException(String message, Throwable cause) {
		super(message, cause);
	}

	public InvalidUnionException(String message) {
		super(message);
	}

	public InvalidUnionException(Throwable cause) {
		super(cause);
	}

}
