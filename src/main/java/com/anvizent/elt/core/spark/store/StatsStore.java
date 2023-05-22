package com.anvizent.elt.core.spark.store;

import java.io.Serializable;
import java.util.ArrayList;

import com.anvizent.elt.core.listener.common.constant.StatsType;

/**
 * @author Hareen Bejjanki
 *
 */
public class StatsStore implements Serializable {

	private static final long serialVersionUID = 1L;

	private StatsType statsType;
	private ArrayList<String> calculatorClasses;
	private ArrayList<ArrayList<String>> constructorArgs;
	private ArrayList<ArrayList<String>> constructorValues;

	public StatsType getStatsType() {
		return statsType;
	}

	public void setStatsType(StatsType statsType) {
		this.statsType = statsType;
	}

	public ArrayList<String> getCalculatorClasses() {
		return calculatorClasses;
	}

	public void setCalculatorClasses(ArrayList<String> calculatorClasses) {
		this.calculatorClasses = calculatorClasses;
	}

	public ArrayList<ArrayList<String>> getConstructorArgs() {
		return constructorArgs;
	}

	public void setConstructorArgs(ArrayList<ArrayList<String>> constructorArgs) {
		this.constructorArgs = constructorArgs;
	}

	public ArrayList<ArrayList<String>> getConstructorValues() {
		return constructorValues;
	}

	public void setConstructorValues(ArrayList<ArrayList<String>> constructorValues) {
		this.constructorValues = constructorValues;
	}
}
