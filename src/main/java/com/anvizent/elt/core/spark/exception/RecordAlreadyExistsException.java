package com.anvizent.elt.core.spark.exception;

import com.anvizent.elt.core.lib.exception.AnvizentFunctionException;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class RecordAlreadyExistsException extends AnvizentFunctionException {

	private static final long serialVersionUID = 1L;

	public RecordAlreadyExistsException() {
		super();
	}

	public RecordAlreadyExistsException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public RecordAlreadyExistsException(String message, Throwable cause) {
		super(message, cause);
	}

	public RecordAlreadyExistsException(String message) {
		super(message);
	}

	public RecordAlreadyExistsException(Throwable cause) {
		super(cause);
	}

}
