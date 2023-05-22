package com.anvizent.elt.core.spark.filter.config.bean;

import java.util.ArrayList;

import com.anvizent.elt.core.spark.operation.config.bean.ResultFetcherConfigBean;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
@SuppressWarnings("rawtypes")
public class FilterByResultConfigBean extends ResultFetcherConfigBean implements IFilterConfigBean {

	private static final long serialVersionUID = 1L;

	private ArrayList<String> emitStreamNames;
	private ArrayList<ArrayList<String>> decodedClassNames = new ArrayList<>();
	private ArrayList<ArrayList<String>> decodedMethods = new ArrayList<>();
	private ArrayList<ArrayList<String>> decodedMethodTypes = new ArrayList<>();
	private ArrayList<ArrayList<ArrayList<String>>> decodedMethodArguments = new ArrayList<>();
	private ArrayList<ArrayList<Class[]>> decodedMethodArgumentTypes = new ArrayList<>();
	private ArrayList<ArrayList<String>> decodedMethodResultTypes = new ArrayList<>();

	@Override
	public ArrayList<String> getEmitStreamNames() {
		return emitStreamNames;
	}

	@Override
	public void setEmitStreamNames(ArrayList<String> emitStreamNames) {
		this.emitStreamNames = emitStreamNames;
	}

	public ArrayList<ArrayList<String>> getDecodedClassNames() {
		return decodedClassNames;
	}

	public void addDecodedClassNames(ArrayList<String> decodedClassNames) {
		this.decodedClassNames.add(decodedClassNames);
	}

	public ArrayList<ArrayList<String>> getDecodedMethods() {
		return decodedMethods;
	}

	public void addDecodedMethods(ArrayList<String> decodedMethods) {
		this.decodedMethods.add(decodedMethods);
	}

	public ArrayList<ArrayList<String>> getDecodedMethodTypes() {
		return decodedMethodTypes;
	}

	public void addDecodedMethodTypes(ArrayList<String> decodedMethodTypes) {
		this.decodedMethodTypes.add(decodedMethodTypes);
	}

	public ArrayList<ArrayList<ArrayList<String>>> getDecodedMethodArguments() {
		return decodedMethodArguments;
	}

	public void addDecodedMethodArguments(ArrayList<ArrayList<String>> decodedMethodArguments) {
		this.decodedMethodArguments.add(decodedMethodArguments);
	}

	public ArrayList<ArrayList<Class[]>> getDecodedMethodArgumentTypes() {
		return decodedMethodArgumentTypes;
	}

	public void addDecodedMethodArgumentTypes(ArrayList<Class[]> decodedMethodArgumentTypes) {
		this.decodedMethodArgumentTypes.add(decodedMethodArgumentTypes);
	}

	public ArrayList<ArrayList<String>> getDecodedMethodResultTypes() {
		return decodedMethodResultTypes;
	}

	public void addDecodedMethodResultTypes(ArrayList<String> decodedMethodResultTypes) {
		this.decodedMethodResultTypes.add(decodedMethodResultTypes);
	}

}
