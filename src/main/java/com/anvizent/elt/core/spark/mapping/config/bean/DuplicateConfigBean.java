package com.anvizent.elt.core.spark.mapping.config.bean;

import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;
import com.anvizent.elt.core.spark.constant.AppendAt;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class DuplicateConfigBean extends MappingConfigBean {

	private static final long serialVersionUID = 1L;

	private boolean everyMeasureAsString;
	private String prefix;
	private String suffix;
	private AppendAt appendAt;

	public boolean isEveryMeasureAsString() {
		return everyMeasureAsString;
	}

	public void setEveryMeasureAsString(boolean measureAsString) {
		this.everyMeasureAsString = measureAsString;
	}

	public String getPrefix() {
		return prefix;
	}

	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	public String getSuffix() {
		return suffix;
	}

	public void setSuffix(String suffix) {
		this.suffix = suffix;
	}

	public AppendAt getAppendAt() {
		return appendAt;
	}

	public void setAppendAt(AppendAt appendAt) {
		this.appendAt = appendAt;
	}

	public DuplicateConfigBean(boolean measureAsString, String prefix, String suffix, AppendAt appendAt) {
		this.everyMeasureAsString = measureAsString;
		this.prefix = prefix;
		this.suffix = suffix;
		this.setAppendAt(appendAt);
	}

}
