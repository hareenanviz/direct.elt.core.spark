package com.anvizent.elt.core.spark.source.resource.config;

import java.io.Serializable;

import com.anvizent.elt.core.spark.resource.config.QuoteConfig;

/**
 * @author Hareen Bejjanki
 *
 */
public class SQLQuoteConfig implements Serializable {
	private static final long serialVersionUID = 1L;

	private QuoteConfig table;
	private QuoteConfig column;
	private QuoteConfig string;
	private QuoteConfig number;
	private QuoteConfig bool;

	public QuoteConfig getTable() {
		return table;
	}

	public void setTable(QuoteConfig table) {
		this.table = table;
	}

	public QuoteConfig getColumn() {
		return column;
	}

	public void setColumn(QuoteConfig column) {
		this.column = column;
	}

	public QuoteConfig getString() {
		return string;
	}

	public void setString(QuoteConfig string) {
		this.string = string;
	}

	public QuoteConfig getNumber() {
		return number;
	}

	public void setNumber(QuoteConfig number) {
		this.number = number;
	}

	public QuoteConfig getBool() {
		return bool;
	}

	public void setBool(QuoteConfig bool) {
		this.bool = bool;
	}

}
