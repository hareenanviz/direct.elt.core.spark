package com.anvizent.elt.core.spark.constant;

import java.util.ArrayList;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public enum CleansingValidationType {

	RANGE("RANGE"), EQUAL("EQUAL"), NOT_EQUAL("NOT_EQUAL"), EMPTY("EMPTY"), NOT_EMPTY("NOT_EMPTY"), MATCHES_REGEX("MATCHES_REGEX"), NOT_MATCHES_REGEX(
			"NOT_MATCHES_REGEX"), CUSTOM_EXPRESSION("CUSTOM_EXPRESSION");

	private String value;

	private CleansingValidationType(String value) {
		this.value = value;
	}

	public static CleansingValidationType getInstance(String value) {
		return getInstance(value, null);
	}

	private static CleansingValidationType getInstance(String value, CleansingValidationType defaultValue) {
		if (value == null || value.isEmpty()) {
			return defaultValue;
		} else if (value.equalsIgnoreCase(RANGE.value)) {
			return RANGE;
		} else if (value.equalsIgnoreCase(EQUAL.value)) {
			return EQUAL;
		} else if (value.equalsIgnoreCase(NOT_EQUAL.value)) {
			return NOT_EQUAL;
		} else if (value.equalsIgnoreCase(EMPTY.value)) {
			return EMPTY;
		} else if (value.equalsIgnoreCase(NOT_EMPTY.value)) {
			return NOT_EMPTY;
		} else if (value.equalsIgnoreCase(MATCHES_REGEX.value)) {
			return MATCHES_REGEX;
		} else if (value.equalsIgnoreCase(NOT_MATCHES_REGEX.value)) {
			return NOT_MATCHES_REGEX;
		} else if (value.equalsIgnoreCase(CUSTOM_EXPRESSION.value)) {
			return CUSTOM_EXPRESSION;
		} else {
			return null;
		}
	}

	public static ArrayList<CleansingValidationType> getInstances(ArrayList<String> values) {
		return getInstances(values, RANGE);
	}

	private static ArrayList<CleansingValidationType> getInstances(ArrayList<String> values, CleansingValidationType defaultValue) {
		ArrayList<CleansingValidationType> cleansingTypes = new ArrayList<>();
		for (String value : values) {
			cleansingTypes.add(getInstance(value));
		}

		return cleansingTypes;
	}
}
