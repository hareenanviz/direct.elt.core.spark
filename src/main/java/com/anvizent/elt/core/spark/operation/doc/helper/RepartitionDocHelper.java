package com.anvizent.elt.core.spark.operation.doc.helper;

import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General.Type;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.Repartition;
import com.anvizent.elt.core.spark.constant.HelpConstants;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.factory.Factory;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class RepartitionDocHelper extends DocHelper {

	public RepartitionDocHelper(Factory factory) throws InvalidParameter {
		super(factory);
	}

	@Override
	public String[] getDescription() {
		return new String[] { "Repartition the data using geiven '" + Repartition.KEY_FIELDS + "'" };
	}

	@Override
	public void addConfigDescriptions() throws InvalidParameter {
		configDescriptionUtil.addConfigDescription(Repartition.KEY_FIELDS, General.YES, "", new String[] { "Partition column name." }, "",
				HelpConstants.Type.LIST_OF_STRINGS);

		configDescriptionUtil.addConfigDescription(Repartition.NUMBER_OF_PARTITIONS, General.YES, "", new String[] { "Number of partitions." }, "",
				Type.INTEGER_AS_STRING);
	}
}
