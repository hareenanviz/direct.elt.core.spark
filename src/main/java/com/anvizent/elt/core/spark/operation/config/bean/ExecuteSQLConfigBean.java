package com.anvizent.elt.core.spark.operation.config.bean;

import java.util.ArrayList;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ExecuteSQLConfigBean extends ConfigBean implements MultiInputOperationConfigBean {

	private static final long serialVersionUID = 1L;

	private ArrayList<String> sourceAliaseNames;
	private String query;

	public ArrayList<String> getSourceAliaseNames() {
		return sourceAliaseNames;
	}

	public void setSourceAliaseNames(ArrayList<String> sourceAliaseNames) {
		this.sourceAliaseNames = sourceAliaseNames;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

}
