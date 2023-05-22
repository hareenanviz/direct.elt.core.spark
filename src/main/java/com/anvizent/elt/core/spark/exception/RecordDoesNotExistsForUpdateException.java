package com.anvizent.elt.core.spark.exception;

import com.anvizent.elt.core.lib.exception.AnvizentFunctionException;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class RecordDoesNotExistsForUpdateException extends AnvizentFunctionException {

	private static final long serialVersionUID = 1L;

	public RecordDoesNotExistsForUpdateException() {
		super();
	}

	public RecordDoesNotExistsForUpdateException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public RecordDoesNotExistsForUpdateException(String message, Throwable cause) {
		super(message, cause);
	}

	public RecordDoesNotExistsForUpdateException(String message) {
		super(message);
	}

	public RecordDoesNotExistsForUpdateException(Throwable cause) {
		super(cause);
	}

}
