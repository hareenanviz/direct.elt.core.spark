package com.anvizent.elt.core.spark.source.resource.config;

import java.io.Serializable;

/**
 * @author Hareen Bejjanki
 *
 */
public class SQLOffsetQuery implements Serializable {
	private static final long serialVersionUID = 1L;

	private String query;
	private int starts;

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public int getStarts() {
		return starts;
	}

	public void setStarts(int starts) {
		this.starts = starts;
	}
}
