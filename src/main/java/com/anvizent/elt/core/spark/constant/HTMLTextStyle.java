package com.anvizent.elt.core.spark.constant;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public enum HTMLTextStyle {
	NONE("<br />", "", "", "", "", "br"), ORDERED_LIST("", "<li>", "</li>", "<ol>", "</ol>", "ol"), UNORDERED_LIST("", "<li>", "</li>", "<ul>", "</ul>", "ul");

	private String lineEnd;
	private String linePrefix;
	private String lineSufix;
	private String prefix;
	private String sufix;
	private String value;

	private HTMLTextStyle(String lineEnd, String linePrefix, String lineSufix, String prefix, String sufix, String value) {
		this.lineEnd = lineEnd;
		this.linePrefix = linePrefix;
		this.lineSufix = lineSufix;
		this.prefix = prefix;
		this.sufix = sufix;
		this.value = value;
	}

	public String getValue() {
		return value;
	}

	public String getLineEnd() {
		return lineEnd;
	}

	public String getLinePrefix() {
		return linePrefix;
	}

	public String getLineSufix() {
		return lineSufix;
	}

	public String getPrefix() {
		return prefix;
	}

	public String getSufix() {
		return sufix;
	}

	public HTMLTextStyle getInstance(String name) {
		return getInstance(name, NONE);
	}

	public HTMLTextStyle getInstance(String name, HTMLTextStyle defaultValue) {
		if (name == null || name.isEmpty()) {
			return defaultValue;
		} else if (name.equalsIgnoreCase("none") || name.equalsIgnoreCase("br")) {
			return NONE;
		} else if (name.equalsIgnoreCase("ordered_list") || name.equalsIgnoreCase("orderedlist") || name.equalsIgnoreCase("ordered list")
				|| name.equalsIgnoreCase("ol")) {
			return ORDERED_LIST;
		} else if (name.equalsIgnoreCase("unordered_list") || name.equalsIgnoreCase("unorderedlist") || name.equalsIgnoreCase("unordered list")
				|| name.equalsIgnoreCase("ul")) {
			return UNORDERED_LIST;
		} else {
			return null;
		}
	}
}