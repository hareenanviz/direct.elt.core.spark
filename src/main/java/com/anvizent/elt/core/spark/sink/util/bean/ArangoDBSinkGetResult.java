package com.anvizent.elt.core.spark.sink.util.bean;

import java.io.Serializable;
import java.util.Map;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ArangoDBSinkGetResult implements Serializable {
	private static final long serialVersionUID = 1L;

	private String key;
	private boolean doUpdate;
	private Map<String, Object> result;

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public boolean isDoUpdate() {
		return doUpdate;
	}

	public void setDoUpdate(boolean doUpdate) {
		this.doUpdate = doUpdate;
	}

	public Map<String, Object> getResult() {
		return result;
	}

	public void setResult(Map<String, Object> result) {
		this.result = result;
	}
}
