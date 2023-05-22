package com.anvizent.elt.core.spark.exception;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class SingleInputNotSupportedException extends Exception {

	private static final long serialVersionUID = 1L;

	public SingleInputNotSupportedException() {
		super();
	}

	public SingleInputNotSupportedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public SingleInputNotSupportedException(String message, Throwable cause) {
		super(message, cause);
	}

	public SingleInputNotSupportedException(String message) {
		super(message);
	}

	public SingleInputNotSupportedException(Throwable cause) {
		super(cause);
	}

}
