package com.anvizent.elt.core.spark.constant;

import java.util.ArrayList;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public enum Aggregations {

	SUM("SUM"), MIN("MIN"), MAX("MAX"), COUNT("COUNT"), COUNT_WITH_NULLS("COUNT_WITH_NULLS"), AVG("AVG"), RANDOM("RANDOM"), JOIN_BY_DELIM("JOIN_BY_DELIM");

	private String value;

	private Aggregations(String value) {
		this.value = value;
	}

	public static Aggregations getInstance(String value) {
		return getInstance(value, COUNT);
	}

	private static Aggregations getInstance(String value, Aggregations defaultValue) {
		if (value == null || value.isEmpty()) {
			return defaultValue;
		} else if (value.equalsIgnoreCase(SUM.value)) {
			return SUM;
		} else if (value.equalsIgnoreCase(MIN.value)) {
			return MIN;
		} else if (value.equalsIgnoreCase(MAX.value)) {
			return MAX;
		} else if (value.equalsIgnoreCase(AVG.value)) {
			return AVG;
		} else if (value.equalsIgnoreCase(COUNT.value)) {
			return COUNT;
		} else if (value.equalsIgnoreCase(COUNT_WITH_NULLS.value)) {
			return COUNT_WITH_NULLS;
		} else if (value.equalsIgnoreCase(RANDOM.value)) {
			return RANDOM;
		} else if (value.equalsIgnoreCase(JOIN_BY_DELIM.value)) {
			return JOIN_BY_DELIM;
		} else {
			return null;
		}
	}

	public static ArrayList<Aggregations> getInstances(ArrayList<String> aggregations) {
		return getInstances(aggregations, COUNT);
	}

	private static ArrayList<Aggregations> getInstances(ArrayList<String> aggregations, Aggregations defaultValue) {
		ArrayList<Aggregations> aggs = new ArrayList<>();
		for (String value : aggregations) {
			aggs.add(getInstance(value, defaultValue));
		}

		return aggs;
	}

}
