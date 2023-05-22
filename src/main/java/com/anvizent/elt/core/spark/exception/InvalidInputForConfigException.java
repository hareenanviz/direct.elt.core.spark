package com.anvizent.elt.core.spark.exception;

import com.anvizent.elt.core.spark.constant.Constants.General;
import com.anvizent.elt.core.spark.util.MutableInteger;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class InvalidInputForConfigException extends Exception {
	private static final long serialVersionUID = 1L;

	public InvalidInputForConfigException() {
		super();
	}

	public InvalidInputForConfigException(String message, String lineNumberMessage, MutableInteger lineNumber, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message + General.CONFIG_SEPARATOR + " " + lineNumberMessage + lineNumber, cause, enableSuppression, writableStackTrace);
	}

	public InvalidInputForConfigException(String message, String lineNumberMessage, MutableInteger lineNumber, Throwable cause) {
		super(message + General.CONFIG_SEPARATOR + " " + lineNumberMessage + lineNumber, cause);
	}

	public InvalidInputForConfigException(String message, String lineNumberMessage, MutableInteger lineNumber) {
		super(message + General.CONFIG_SEPARATOR + " " + lineNumberMessage + lineNumber);
	}

	public InvalidInputForConfigException(String message) {
		super(message);
	}

	public InvalidInputForConfigException(Throwable cause) {
		super(cause);
	}

}
