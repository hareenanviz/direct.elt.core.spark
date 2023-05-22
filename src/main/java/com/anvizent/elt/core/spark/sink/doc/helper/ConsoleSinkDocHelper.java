package com.anvizent.elt.core.spark.sink.doc.helper;

import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.factory.Factory;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ConsoleSinkDocHelper extends DocHelper {

	public ConsoleSinkDocHelper(Factory factory) throws InvalidParameter {
		super(factory);
	}

	@Override
	public String[] getDescription() {
		return new String[] { "Write the data into console(log) along with the structure." };
	}

	@Override
	public void addConfigDescriptions() throws InvalidParameter {
		// TODO Auto-generated method stub

	}

}
