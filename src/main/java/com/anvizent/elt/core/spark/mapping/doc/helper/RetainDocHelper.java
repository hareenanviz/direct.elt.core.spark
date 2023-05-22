package com.anvizent.elt.core.spark.mapping.doc.helper;

import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Mapping.Retain;
import com.anvizent.elt.core.spark.constant.HelpConstants.Type;
import com.anvizent.elt.core.spark.exception.InvalidParameter;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class RetainDocHelper extends MappingDocHelper {

	public RetainDocHelper() throws InvalidParameter {
		super();
	}

	@Override
	public void addConfigDescriptions() throws InvalidParameter {
		configDescriptionUtil.addConfigDescription(Retain.RETAIN_FIELDS, General.YES, "", new String[] { "Retain field names" }, "", Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(Retain.RETAIN_AS, General.NO, "", new String[] { "Retain field's alias names" }, "", Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(Retain.RETAIN_AT, General.NO, "", new String[] { "Retain fields at provided index" }, "",
				Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(Retain.RETAIN_EMIT, General.YES, "", new String[] { "Emit field names" }, "", Type.LIST_OF_STRINGS);
	}
}
