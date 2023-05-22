package com.anvizent.elt.core.spark.operation.doc.helper;

import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.ExecuteSQL;
import com.anvizent.elt.core.spark.constant.HelpConstants.Type;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.factory.Factory;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ExecuteSQLDocHelper extends DocHelper {

	public ExecuteSQLDocHelper(Factory factory) throws InvalidParameter {
		super(factory);
	}

	@Override
	public String[] getDescription() {
		return new String[] {
				"This component is to help SQL handson users. This takes a select query which may contain aggregations, joins, SQL function, etc." };
	}

	@Override
	public void addConfigDescriptions() throws InvalidParameter {
		configDescriptionUtil.addConfigDescription(ExecuteSQL.SOURCE_ALIASE_NAMES, General.YES, "", new String[] { "Alias names for all the input sources" },
				"", Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(ExecuteSQL.QUERY, General.YES, "", new String[] { "Query to be execute using input sources." });
	}

}
