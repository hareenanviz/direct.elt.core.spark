package com.anvizent.elt.core.spark.constant;

import java.util.ArrayList;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public enum Granularity {

		SECOND("SECOND"), MINUTE("MINUTE"), HOUR("HOUR"), DAY("DAY"), MONTH("MONTH"), YEAR("YEAR");

	private String value;

	private Granularity(String value) {
		this.value = value;
	}

	public static Granularity getInstance(String value) {
		return getInstance(value, SECOND);
	}

	private static Granularity getInstance(String value, Granularity defaultValue) {
		if (value == null || value.isEmpty()) {
			return defaultValue;
		} else if (value.equalsIgnoreCase(SECOND.value)) {
			return SECOND;
		} else if (value.equalsIgnoreCase(MINUTE.value)) {
			return MINUTE;
		} else if (value.equalsIgnoreCase(HOUR.value)) {
			return HOUR;
		} else if (value.equalsIgnoreCase(DAY.value)) {
			return DAY;
		} else if (value.equalsIgnoreCase(MONTH.value)) {
			return MONTH;
		} else if (value.equalsIgnoreCase(YEAR.value)) {
			return YEAR;
		} else {
			return null;
		}
	}

	public static ArrayList<Granularity> getInstances(ArrayList<String> granularities) {
		return getInstances(granularities, SECOND);
	}

	private static ArrayList<Granularity> getInstances(ArrayList<String> granularitiesAsString, Granularity defaultValue) {
		ArrayList<Granularity> granularities = new ArrayList<>();
		for (String value : granularitiesAsString) {
			granularities.add(getInstance(value));
		}

		return granularities;
	}

}
