package com.anvizent.elt.core.spark.mapping.doc.helper;

import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Mapping.Replicate;
import com.anvizent.elt.core.spark.constant.HelpConstants.Type;
import com.anvizent.elt.core.spark.exception.InvalidParameter;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ReplicateDocHelper extends MappingDocHelper {

	public ReplicateDocHelper() throws InvalidParameter {
		super();
	}

	@Override
	public void addConfigDescriptions() throws InvalidParameter {
		configDescriptionUtil.addConfigDescription(Replicate.REPLICATE_FIELDS, General.YES, "", new String[] { "Replicate field names" }, "",
				Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(Replicate.REPLICATE_TO_FIELDS, General.YES, "", new String[] { "Replicate to field names" },
				"Number of to fields should be equal to number of from fields.", Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(Replicate.REPLICATE_POSITIONS, General.YES, "", new String[] { "Replicate field positions" },
				"Number of positions should be equal to number of from fields.", Type.LIST_OF_INTEGERS);
	}
}
