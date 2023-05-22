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
public class RethinkFetcherDocHelper extends RethinkRetrievalDocHelper {

	public RethinkFetcherDocHelper(Factory factory) throws InvalidParameter {
		super(factory);
	}

	@Override
	public String[] getDescription() {
		return new String[] { "Fetches and joins one or more records from given RethinkDB table based on values matching for the bellow criterias. ",
				"Joins with null values if no record is fetched and '" + Operation.General.ON_ZERO_FETCH + " = " + OnZeroFetchOperation.IGNORE + "'" };
	}

	@Override
	protected void addRethinkLRetrievalConfigDescription() throws InvalidParameter {
		configDescriptionUtil.addConfigDescription(Operation.General.MAX_FETCH_LIMIT, General.YES, "",
				new String[] { "Limits maximum number of fetch records." }, "", "Integer");
	}
}
