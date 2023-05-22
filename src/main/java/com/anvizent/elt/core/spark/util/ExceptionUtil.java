package com.anvizent.elt.core.spark.util;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ExceptionUtil {

	public static void addCause(Throwable throwable, Throwable cause) {
		if (throwable != null && cause != null) {
			Throwable innerCause = throwable;
			Throwable tempCause = null;

			while ((tempCause = throwable.getCause()) != null) {
				innerCause = tempCause;
			}

			innerCause.initCause(cause);
		}
	}

}
