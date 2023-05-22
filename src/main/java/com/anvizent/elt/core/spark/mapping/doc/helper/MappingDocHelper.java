package com.anvizent.elt.core.spark.mapping.doc.helper;

import com.anvizent.elt.core.spark.config.ComponentConfigDoc;
import com.anvizent.elt.core.spark.config.util.ConfigDescriptionUtil;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.util.IndexedAndLinkedMap;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public abstract class MappingDocHelper {
	protected ConfigDescriptionUtil configDescriptionUtil = new ConfigDescriptionUtil();

	public MappingDocHelper() throws InvalidParameter {
		addConfigDescriptions();
	}

	public IndexedAndLinkedMap<String, ComponentConfigDoc> getConfigDescriptions() {
		return configDescriptionUtil.getConfigDescriptions();
	}

	public abstract void addConfigDescriptions() throws InvalidParameter;

}