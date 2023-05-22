package com.anvizent.elt.core.spark.exception;

import com.anvizent.elt.core.lib.exception.RecordProcessingException;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class CleansingValidationException extends RecordProcessingException {

	private static final long serialVersionUID = 1L;

	public CleansingValidationException() {
		super();
	}

	public CleansingValidationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public CleansingValidationException(String message, Throwable cause) {
		super(message, cause);
	}

	public CleansingValidationException(String message) {
		super(message);
	}

	public CleansingValidationException(Throwable cause) {
		super(cause);
	}

}
