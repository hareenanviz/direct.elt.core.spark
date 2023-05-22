package com.anvizent.elt.core.spark.operation.doc.helper;

import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation;
import com.anvizent.elt.core.spark.constant.OnZeroFetchOperation;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.factory.Factory;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class RethinkLookUpDocHelper extends RethinkRetrievalDocHelper {

	public RethinkLookUpDocHelper(Factory factory) throws InvalidParameter {
		super(factory);
	}

	@Override
	public String[] getDescription() {
		return new String[] { "Looksup for a record from given RethinkDB table based on values matching for the bellow criterias. ",
				"Joins with null values if no record is fetched and '" + Operation.General.ON_ZERO_FETCH + " = " + OnZeroFetchOperation.IGNORE + "'" };
	}

	@Override
	protected void addRethinkLRetrievalConfigDescription() throws InvalidParameter {
		configDescriptionUtil.addConfigDescription(Operation.General.LIMIT_TO_1, General.NO, "False",
				new String[] { "Limits records to 1 if set to 'true' and throws exception if found more than 1 record." }, "", "Boolean");
	}
}
