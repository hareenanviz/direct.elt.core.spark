package com.anvizent.elt.core.spark.exception;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class TableDoesNotExistsException extends Exception {

	private static final long serialVersionUID = 1L;

	public TableDoesNotExistsException() {
		super();
	}

	public TableDoesNotExistsException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public TableDoesNotExistsException(String message, Throwable cause) {
		super(message, cause);
	}

	public TableDoesNotExistsException(String message) {
		super(message);
	}

	public TableDoesNotExistsException(Throwable cause) {
		super(cause);
	}
}
