package com.anvizent.elt.core.spark.exception;

import com.anvizent.elt.core.lib.exception.RecordProcessingException;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class InvalidLookUpException extends RecordProcessingException {

	private static final long serialVersionUID = 1L;

	public InvalidLookUpException() {
		super();
	}

	public InvalidLookUpException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public InvalidLookUpException(String message, Throwable cause) {
		super(message, cause);
	}

	public InvalidLookUpException(String message) {
		super(message);
	}

	public InvalidLookUpException(Throwable cause) {
		super(cause);
	}
}
