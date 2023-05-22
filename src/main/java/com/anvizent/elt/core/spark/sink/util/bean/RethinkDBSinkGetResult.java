package com.anvizent.elt.core.spark.sink.util.bean;

import java.io.Serializable;
import java.util.Map;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class RethinkDBSinkGetResult implements Serializable {
	private static final long serialVersionUID = 1L;

	private Object rId;
	private boolean doUpdate;
	private Map<String, Object> result;

	public Object getrId() {
		return rId;
	}

	public void setrId(Object rId) {
		this.rId = rId;
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
