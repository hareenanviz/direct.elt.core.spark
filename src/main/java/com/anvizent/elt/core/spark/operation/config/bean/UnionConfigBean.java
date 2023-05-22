package com.anvizent.elt.core.spark.operation.config.bean;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class UnionConfigBean extends ConfigBean implements MultiInputOperationConfigBean {

	private static final long serialVersionUID = 1L;

	private String structureSourceName;

	public String getStructureSourceName() {
		return structureSourceName;
	}

	public void setStructureSourceName(String structureSourceName) {
		this.structureSourceName = structureSourceName;
	}
}
