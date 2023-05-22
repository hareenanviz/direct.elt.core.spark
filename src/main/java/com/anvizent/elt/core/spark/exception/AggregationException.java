package com.anvizent.elt.core.spark.exception;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class AggregationException extends Exception {

	private static final long serialVersionUID = 1L;

	public AggregationException() {
		super();
	}

	public AggregationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public AggregationException(String message, Throwable cause) {
		super(message, cause);
	}

	public AggregationException(String message) {
		super(message);
	}

	@SuppressWarnings("rawtypes")
	public AggregationException(String aggregationField, Class javaType) {
		super("Unsupported aggregation operation on field: " + aggregationField + " and type: " + javaType.getName());
	}

	public AggregationException(Throwable cause) {
		super(cause);
	}

}
