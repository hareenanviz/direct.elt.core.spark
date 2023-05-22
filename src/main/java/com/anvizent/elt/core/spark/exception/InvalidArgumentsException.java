package com.anvizent.elt.core.spark.exception;

import com.anvizent.elt.core.spark.constant.Constants.General;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class InvalidArgumentsException extends Exception {

	private static final long serialVersionUID = 1L;

	private String message;
	private int numberOfExceptions = 0;

	public InvalidArgumentsException() {
		this.message = "Invalid Arguments Exception";
	}

	public InvalidArgumentsException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
		numberOfExceptions++;
	}

	public InvalidArgumentsException(String message, Throwable cause) {
		super(message, cause);
		numberOfExceptions++;
	}

	public InvalidArgumentsException(String message) {
		super(message);
		numberOfExceptions++;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public int getNumberOfExceptions() {
		return numberOfExceptions;
	}

	public void add(String message) {
		this.message += General.NEW_LINE + General.TAB + numberOfExceptions++ + General.COLON + " " + message;
	}
}
