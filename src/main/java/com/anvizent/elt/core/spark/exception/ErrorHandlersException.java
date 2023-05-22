package com.anvizent.elt.core.spark.exception;

import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ErrorHandlersException extends InvalidConfigException {

	private static final long serialVersionUID = 1L;

	public ErrorHandlersException() {
		super();
	}

	public ErrorHandlersException(String message) {
		baseMessage = message;
	}

}
