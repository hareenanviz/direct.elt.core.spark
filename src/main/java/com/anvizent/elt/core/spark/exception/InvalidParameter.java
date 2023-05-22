package com.anvizent.elt.core.spark.exception;

import com.anvizent.elt.core.spark.constant.Constants.ExceptionMessage;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class InvalidParameter extends Exception {
	private static final long serialVersionUID = 1L;

	public InvalidParameter(String paramNameOrMessage, boolean mandatory) {
		super(mandatory ? paramNameOrMessage + ExceptionMessage.IS_MANDATORY : paramNameOrMessage);
	}
}
