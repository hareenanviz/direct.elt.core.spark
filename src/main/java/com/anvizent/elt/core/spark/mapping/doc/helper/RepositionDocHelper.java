package com.anvizent.elt.core.spark.mapping.doc.helper;

import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Mapping.Reposition;
import com.anvizent.elt.core.spark.constant.HelpConstants.Type;
import com.anvizent.elt.core.spark.exception.InvalidParameter;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class RepositionDocHelper extends MappingDocHelper {

	public RepositionDocHelper() throws InvalidParameter {
		super();
	}

	@Override
	public void addConfigDescriptions() throws InvalidParameter {
		configDescriptionUtil.addConfigDescription(Reposition.FIELDS, General.YES, "", new String[] { "Field names need to be repositioned." }, "",
				Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(Reposition.POSITIONS, General.YES, "",
				new String[] { "New position indexes. Positive indexes starts from 0 and negative indexes starts from -1." },
				"Number of " + Reposition.POSITIONS + " should be equal to number of " + Reposition.FIELDS + ".", Type.LIST_OF_INTEGERS);
	}
}
