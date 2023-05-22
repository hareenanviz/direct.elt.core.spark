package com.anvizent.elt.core.spark.operation.pojo;

import java.lang.reflect.Method;

public class MethodInfo {
	private Object object;
	private Method method;
	private Integer varArgsIndex;
	private Class<?> varArgsType;

	public Object getObject() {
		return object;
	}

	public void setObject(Object object) {
		this.object = object;
	}

	public Method getMethod() {
		return method;
	}

	public void setMethod(Method method) {
		this.method = method;
	}

	public Integer getVarArgsIndex() {
		return varArgsIndex;
	}

	public void setVarArgsIndex(Integer varArgsIndex) {
		this.varArgsIndex = varArgsIndex;
	}

	public Class<?> getVarArgsType() {
		return varArgsType;
	}

	public void setVarArgsType(Class<?> varArgsType) {
		this.varArgsType = varArgsType;
	}

	public MethodInfo() {
	}

	public MethodInfo(Method method, Integer varArgsIndex, Class<?> varArgsType) {
		this.method = method;
		this.varArgsIndex = varArgsIndex;
		this.varArgsType = varArgsType;
	}
}
