package com.anvizent.elt.core.spark.config;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ConfigurationReadingException extends Exception {
	private static final long serialVersionUID = 1L;

	public ConfigurationReadingException() {
		super();
	}

	public ConfigurationReadingException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ConfigurationReadingException(String message, Throwable cause) {
		super(message, cause);
	}

	public ConfigurationReadingException(String message) {
		super(message);
	}

	public ConfigurationReadingException(Throwable cause) {
		super(cause);
	}

}
