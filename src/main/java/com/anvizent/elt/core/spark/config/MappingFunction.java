package com.anvizent.elt.core.spark.config;

import java.io.Serializable;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class MappingFunction implements Serializable {
	private static final long serialVersionUID = 1L;

	private String name;
	private String allowedFor;
	private String description;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getAllowedFor() {
		return allowedFor;
	}

	public void setAllowedFor(String allowedFor) {
		this.allowedFor = allowedFor;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public MappingFunction(String name, String allowedFor, String description) {
		this.name = name;
		this.allowedFor = allowedFor;
		this.description = description;
	}

}
