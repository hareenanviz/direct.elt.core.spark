package com.anvizent.elt.core.spark.exception;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ConfigReferenceNotFoundException extends Exception {

	private static final long serialVersionUID = 1L;

	public ConfigReferenceNotFoundException() {
		super();
	}

	public ConfigReferenceNotFoundException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ConfigReferenceNotFoundException(String message, Throwable cause) {
		super(message, cause);
	}

	public ConfigReferenceNotFoundException(String message) {
		super(message);
	}

	public ConfigReferenceNotFoundException(Throwable cause) {
		super(cause);
	}
}
