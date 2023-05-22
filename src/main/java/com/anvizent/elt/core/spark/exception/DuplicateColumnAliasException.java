package com.anvizent.elt.core.spark.exception;

import com.anvizent.elt.core.lib.exception.RecordProcessingException;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class DuplicateColumnAliasException extends RecordProcessingException {

	private static final long serialVersionUID = 1L;

	public DuplicateColumnAliasException() {
		super();
	}

	public DuplicateColumnAliasException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public DuplicateColumnAliasException(String message, Throwable cause) {
		super(message, cause);
	}

	public DuplicateColumnAliasException(String message) {
		super(message);
	}

	public DuplicateColumnAliasException(Throwable cause) {
		super(cause);
	}

}
