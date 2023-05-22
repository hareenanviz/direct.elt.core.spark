package com.anvizent.elt.core.spark.exception;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class MultipleInputsNotSupportedException extends Exception {

	private static final long serialVersionUID = 1L;

	public MultipleInputsNotSupportedException() {
		super();
	}

	public MultipleInputsNotSupportedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public MultipleInputsNotSupportedException(String message, Throwable cause) {
		super(message, cause);
	}

	public MultipleInputsNotSupportedException(String message) {
		super(message);
	}

	public MultipleInputsNotSupportedException(Throwable cause) {
		super(cause);
	}

}
