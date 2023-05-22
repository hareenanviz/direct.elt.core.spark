package com.anvizent.elt.core.spark.operation.exception;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class MLMResponseException extends Exception {
	private static final long serialVersionUID = 1L;

	public MLMResponseException() {
		super();
	}

	public MLMResponseException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public MLMResponseException(String message, Throwable cause) {
		super(message, cause);
	}

	public MLMResponseException(String message) {
		super(message);
	}

	public MLMResponseException(Throwable cause) {
		super(cause);
	}
}
