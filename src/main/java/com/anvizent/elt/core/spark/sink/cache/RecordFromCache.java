package com.anvizent.elt.core.spark.sink.cache;

import java.io.Serializable;
import java.util.ArrayList;

public class RecordFromCache implements Serializable {
	private static final long serialVersionUID = 1L;

	private boolean found;
	private boolean extracted;
	private ArrayList<Object> value;

	public boolean isFound() {
		return found;
	}

	public void setFound(boolean found) {
		this.found = found;
	}

	public boolean isExtracted() {
		return extracted;
	}

	public void setExtracted(boolean extracted) {
		this.extracted = extracted;
	}

	public ArrayList<Object> getValue() {
		return value;
	}

	public void setValue(ArrayList<Object> value) {
		this.value = value;
	}

	public RecordFromCache() {
	}

	public RecordFromCache(boolean found, boolean extracted, ArrayList<Object> value) {
		this.found = found;
		this.value = value;
		this.extracted = extracted;
	}
}
