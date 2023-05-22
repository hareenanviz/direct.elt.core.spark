package com.anvizent.elt.core.spark.mapping.doc.helper;

import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Mapping.Rename;
import com.anvizent.elt.core.spark.constant.HelpConstants;
import com.anvizent.elt.core.spark.exception.InvalidParameter;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class RenameDocHelper extends MappingDocHelper {

	public RenameDocHelper() throws InvalidParameter {
		super();
	}

	@Override
	public void addConfigDescriptions() throws InvalidParameter {
		configDescriptionUtil.addConfigDescription(Rename.RENAME_FROM, General.YES, "", new String[] { "Rename from field names" }, "",
				HelpConstants.Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(Rename.RENAME_TO, General.YES, "", new String[] { "Rename to field names" },
				"Number of rename to fields should be equal to number of rename from fields.", HelpConstants.Type.LIST_OF_STRINGS);
	}
}
