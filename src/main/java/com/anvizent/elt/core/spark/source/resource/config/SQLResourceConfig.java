package com.anvizent.elt.core.spark.source.resource.config;

import java.io.Serializable;

/**
 * @author Hareen Bejjanki
 *
 */
public class SQLResourceConfig implements Serializable {
	private static final long serialVersionUID = 1L;

	private SQLQuoteConfig quoteConfig;
	private SQLOffsetQueries offSetQuery;
	private SQLTableSizeQueries tableSizeQuery;

	public SQLQuoteConfig getQuoteConfig() {
		return quoteConfig;
	}

	public void setQuoteConfig(SQLQuoteConfig quoteConfig) {
		this.quoteConfig = quoteConfig;
	}

	public SQLOffsetQueries getOffSetQuery() {
		return offSetQuery;
	}

	public void setOffSetQuery(SQLOffsetQueries offSetQuery) {
		this.offSetQuery = offSetQuery;
	}

	public SQLTableSizeQueries getTableSizeQuery() {
		return tableSizeQuery;
	}

	public void setTableSizeQuery(SQLTableSizeQueries tableSizeQuery) {
		this.tableSizeQuery = tableSizeQuery;
	}

}
