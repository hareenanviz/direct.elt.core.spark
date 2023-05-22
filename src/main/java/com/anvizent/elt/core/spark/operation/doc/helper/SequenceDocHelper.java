package com.anvizent.elt.core.spark.operation.doc.helper;

import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.factory.Factory;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class SequenceDocHelper extends DocHelper {
	public SequenceDocHelper(Factory factory) throws InvalidParameter {
		super(factory);
	}

	@Override
	public String[] getDescription() {
		return new String[] { "Generates sequence for each record in the data." };
	}

	@Override
	public void addConfigDescriptions() throws InvalidParameter {
		configDescriptionUtil.addConfigDescription(General.FIELDS, General.YES, "", new String[] { General.FIELDS });
		configDescriptionUtil.addConfigDescription(Operation.Sequence.INITIAL_VALUES, General.NO, "", new String[] { Operation.Sequence.INITIAL_VALUES },
				"Number of " + Operation.Sequence.INITIAL_VALUES + " provided should be equals to number of " + General.FIELDS + ".");
		configDescriptionUtil.addConfigDescription(General.FIELD_INDEXES, General.NO, "", new String[] { General.FIELD_INDEXES },
				"Number of " + General.FIELD_INDEXES + " provided should be equals to number of " + General.FIELDS + ".");
	}
}