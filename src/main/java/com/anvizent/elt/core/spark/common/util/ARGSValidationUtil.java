package com.anvizent.elt.core.spark.common.util;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.spark.constant.Constants.ARGSConstant;
import com.anvizent.elt.core.spark.exception.InvalidArgumentsException;
import com.anvizent.util.ArgsUtil;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ARGSValidationUtil {

	public static String getHelpValue(LinkedHashMap<String, ArrayList<String>> arguments, InvalidArgumentsException exception) {
		if (arguments.containsKey(ARGSConstant.HELP)) {
			if (arguments.get(ARGSConstant.HELP) == null || arguments.get(ARGSConstant.HELP).isEmpty() || arguments.get(ARGSConstant.HELP).get(0) == null
					|| arguments.get(ARGSConstant.HELP).get(0).isEmpty()) {
				exception.add(getMandatoryMessage(ARGSConstant.HELP));
			} else if (!ARGSConstant.VALID_HELP_VALUES.contains(arguments.get(ARGSConstant.HELP).get(0).toLowerCase())) {
				exception.add("Invalid argument value '" + arguments.get(ARGSConstant.HELP).get(0) + "' for " + ARGSConstant.HELP + ".");
			} else {
				if (arguments.get(ARGSConstant.HELP).get(0).equalsIgnoreCase(ARGSConstant.HTML)) {
					return ARGSConstant.HTML;
				} else if (arguments.get(ARGSConstant.HELP).get(0).equalsIgnoreCase(ARGSConstant.CONSOLE)) {
					return ARGSConstant.CONSOLE;
				}
			}
		}

		return null;
	}

	public static void validateMandatoryARGS(LinkedHashMap<String, ArrayList<String>> arguments, InvalidArgumentsException exception) throws Exception {
		if (arguments.get(ARGSConstant.APP_NAME) == null || arguments.get(ARGSConstant.APP_NAME).isEmpty()
				|| arguments.get(ARGSConstant.APP_NAME).get(0) == null || arguments.get(ARGSConstant.APP_NAME).get(0).isEmpty()) {
			exception.add(getMandatoryMessage(ARGSConstant.APP_NAME));
		}

		if (arguments.get(ARGSConstant.CONFIGS) == null || arguments.get(ARGSConstant.CONFIGS).isEmpty() || arguments.get(ARGSConstant.CONFIGS).get(0) == null
				|| arguments.get(ARGSConstant.CONFIGS).get(0).isEmpty()) {
			exception.add(getMandatoryMessage(ARGSConstant.CONFIGS));
		}

		if (arguments.get(ARGSConstant.VALUES) == null || arguments.get(ARGSConstant.VALUES).isEmpty() || arguments.get(ARGSConstant.VALUES).get(0) == null
				|| arguments.get(ARGSConstant.VALUES).get(0).isEmpty()) {
			exception.add(getMandatoryMessage(ARGSConstant.VALUES));
		}

		if (isAnyArgumentPresent(arguments, ARGSConstant.JDBC_URL, ARGSConstant.DRIVER, ARGSConstant.USER_NAME, ARGSConstant.PASSWORD)) {
			validateRDBMSConnectionARGS(arguments, exception);
		}

		if (isAnyArgumentPresent(arguments, ARGSConstant.PRIVATE_KEY, ARGSConstant.IV)) {
			validateEncryptionARGS(arguments, exception);
		}
	}

	private static void validateRDBMSConnectionARGS(LinkedHashMap<String, ArrayList<String>> arguments, InvalidArgumentsException exception) {
		if (arguments.get(ARGSConstant.JDBC_URL) == null || arguments.get(ARGSConstant.JDBC_URL).isEmpty()
				|| arguments.get(ARGSConstant.JDBC_URL).get(0) == null || arguments.get(ARGSConstant.JDBC_URL).get(0).isEmpty()) {
			exception.add(getMandatoryMessage(ARGSConstant.JDBC_URL));
		}

		if (arguments.get(ARGSConstant.DRIVER) == null || arguments.get(ARGSConstant.DRIVER).isEmpty() || arguments.get(ARGSConstant.DRIVER).get(0) == null
				|| arguments.get(ARGSConstant.DRIVER).get(0).isEmpty()) {
			exception.add(getMandatoryMessage(ARGSConstant.DRIVER));
		}

		if (arguments.get(ARGSConstant.USER_NAME) == null || arguments.get(ARGSConstant.USER_NAME).isEmpty()
				|| arguments.get(ARGSConstant.USER_NAME).get(0) == null || arguments.get(ARGSConstant.USER_NAME).get(0).isEmpty()) {
			exception.add(getMandatoryMessage(ARGSConstant.USER_NAME));
		}

		if (arguments.get(ARGSConstant.PASSWORD) == null || arguments.get(ARGSConstant.PASSWORD).isEmpty()
				|| arguments.get(ARGSConstant.PASSWORD).get(0) == null || arguments.get(ARGSConstant.PASSWORD).get(0).isEmpty()) {
			exception.add(getMandatoryMessage(ARGSConstant.PASSWORD));
		}
	}

	private static String getMandatoryMessage(String key) {
		return ArgsUtil.DEFAULT_CHARS_BEFORE_CONTEXT_KEY + key + " is mandatory.";
	}

	private static void validateEncryptionARGS(LinkedHashMap<String, ArrayList<String>> arguments, InvalidArgumentsException exception) {
		if (arguments.get(ARGSConstant.PRIVATE_KEY) == null || arguments.get(ARGSConstant.PRIVATE_KEY).isEmpty()
				|| arguments.get(ARGSConstant.PRIVATE_KEY).get(0) == null || arguments.get(ARGSConstant.PRIVATE_KEY).get(0).isEmpty()) {
			exception.add(getMandatoryMessage(ARGSConstant.PRIVATE_KEY));
		}

		if (arguments.get(ARGSConstant.IV) == null || arguments.get(ARGSConstant.IV).isEmpty() || arguments.get(ARGSConstant.IV).get(0) == null
				|| arguments.get(ARGSConstant.IV).get(0).isEmpty()) {
			exception.add(getMandatoryMessage(ARGSConstant.IV));
		}
	}

	private static boolean isAnyArgumentPresent(LinkedHashMap<String, ArrayList<String>> arguments, String... keys) {
		for (int i = 0; i < keys.length; i++) {
			if (arguments.get(keys[i]) != null && !arguments.get(keys[i]).isEmpty() && arguments.get(keys[i]).get(0) != null
					&& arguments.get(keys[i]).get(0).isEmpty()) {
				return true;
			}
		}

		return false;
	}
}
