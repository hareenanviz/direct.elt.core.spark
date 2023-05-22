package com.anvizent.elt.core.spark.constant;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public enum JoinType {

	SIMPLE_JOIN("SIMPLE_JOIN"), LEFT_OUTER_JOIN("LEFT_OUTER_JOIN"), RIGHT_OUTER_JOIN("RIGHT_OUTER_JOIN"), FULL_OUTER_JOIN("FULL_OUTER_JOIN");

	private String value;

	private JoinType(String value) {
		this.value = value;
	}

	public static JoinType getInstance(String value) {
		return getInstance(value, JoinType.SIMPLE_JOIN);
	}

	private static JoinType getInstance(String value, JoinType defaultValue) {
		if (value == null || value.isEmpty()) {
			return defaultValue;
		} else if (value.equalsIgnoreCase(SIMPLE_JOIN.value)) {
			return SIMPLE_JOIN;
		} else if (value.equalsIgnoreCase(LEFT_OUTER_JOIN.value)) {
			return LEFT_OUTER_JOIN;
		} else if (value.equalsIgnoreCase(RIGHT_OUTER_JOIN.value)) {
			return RIGHT_OUTER_JOIN;
		} else if (value.equalsIgnoreCase(FULL_OUTER_JOIN.value)) {
			return FULL_OUTER_JOIN;
		} else {
			return null;
		}

	}

}
