package com.anvizent.elt.core.spark.exception;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class TableAlreadyExistsException extends Exception {

	private static final long serialVersionUID = 1L;

	public TableAlreadyExistsException() {
		super();
	}

	public TableAlreadyExistsException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public TableAlreadyExistsException(String message, Throwable cause) {
		super(message, cause);
	}

	public TableAlreadyExistsException(String message) {
		super(message);
	}

	public TableAlreadyExistsException(Throwable cause) {
		super(cause);
	}
}
