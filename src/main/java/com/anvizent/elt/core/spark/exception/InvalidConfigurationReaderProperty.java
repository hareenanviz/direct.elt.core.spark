package com.anvizent.elt.core.spark.exception;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class InvalidConfigurationReaderProperty extends Exception {
	private static final long serialVersionUID = 1L;

	public InvalidConfigurationReaderProperty() {
		super();
	}

	public InvalidConfigurationReaderProperty(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public InvalidConfigurationReaderProperty(String message, Throwable cause) {
		super(message, cause);
	}

	public InvalidConfigurationReaderProperty(String message) {
		super(message);
	}

	public InvalidConfigurationReaderProperty(Throwable cause) {
		super(cause);
	}

}
