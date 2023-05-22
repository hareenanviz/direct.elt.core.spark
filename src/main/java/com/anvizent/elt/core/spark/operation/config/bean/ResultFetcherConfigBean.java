package com.anvizent.elt.core.spark.operation.config.bean;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
@SuppressWarnings("rawtypes")
public class ResultFetcherConfigBean extends ConfigBean implements SimpleOperationConfigBean {

	private static final long serialVersionUID = 1L;

	private ArrayList<String> classNames;
	private ArrayList<String> methodNames;
	private ArrayList<Integer> varArgsIndexes;
	private ArrayList<String> methodTypes = new ArrayList<>();
	private ArrayList<ArrayList<String>> methodArgumentFields;
	private ArrayList<Class[]> methodArgumentFieldTypes = new ArrayList<>();
	private ArrayList<String> returnFields;
	private ArrayList<Integer> returnFieldsIndexes;
	private Map<String, Object> classObjects = new HashMap<>();
	private Map<String, Class> classes = new HashMap<>();

	public ArrayList<String> getClassNames() {
		return classNames;
	}

	public void setClassNames(ArrayList<String> classNames) {
		this.classNames = classNames;
	}

	public ArrayList<String> getMethodNames() {
		return methodNames;
	}

	public void setMethodNames(ArrayList<String> methodNames) {
		this.methodNames = methodNames;
	}

	public Integer getVarArgsIndexes(int i) {
		return varArgsIndexes != null && !varArgsIndexes.isEmpty() ? varArgsIndexes.get(i) : null;
	}

	public ArrayList<Integer> getVarArgsIndexes() {
		return varArgsIndexes;
	}

	public void setVarArgsIndexes(ArrayList<Integer> varArgsIndexes) {
		this.varArgsIndexes = varArgsIndexes;
	}

	public ArrayList<String> getMethodTypes() {
		return methodTypes;
	}

	public void addMethodTypes(String methodType) {
		this.methodTypes.add(methodType);
	}

	public ArrayList<ArrayList<String>> getMethodArgumentFields() {
		return methodArgumentFields;
	}

	public void setMethodArgumentFields(ArrayList<ArrayList<String>> methodArgumentFields) {
		this.methodArgumentFields = methodArgumentFields;
	}

	public ArrayList<Class[]> getMethodArgumentFieldTypes() {
		return methodArgumentFieldTypes;
	}

	public void addMethodArgumentFieldTypes(Class[] methodArgumentFieldTypes) {
		this.methodArgumentFieldTypes.add(methodArgumentFieldTypes);
	}

	public ArrayList<String> getReturnFields() {
		return returnFields;
	}

	public void setReturnFields(ArrayList<String> returnFields) {
		this.returnFields = returnFields;
	}

	public ArrayList<Integer> getReturnFieldsIndexes() {
		return returnFieldsIndexes;
	}

	public void setReturnFieldsIndexes(ArrayList<Integer> returnFieldsIndexes) {
		this.returnFieldsIndexes = returnFieldsIndexes;
	}

	public Map<String, Object> getClassObjects() {
		return classObjects;
	}

	public Object getClassObject(String className) {
		return classObjects.get(className);
	}

	public void putClassObject(String className, Object classObject) {
		if (!classObjects.containsKey(className)) {
			classObjects.put(className, classObject);
		}
	}

	public Class getClass(String className) {
		return classes.get(className);
	}

	public void putClass(String className, Class value) {
		if (!classes.containsKey(className)) {
			classes.put(className, value);
		}
	}

}
