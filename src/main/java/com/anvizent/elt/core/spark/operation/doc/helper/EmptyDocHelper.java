package com.anvizent.elt.core.spark.operation.doc.helper;

import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.factory.Factory;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class EmptyDocHelper extends DocHelper {

	public EmptyDocHelper(Factory factory) throws InvalidParameter {
		super(factory);
	}

	@Override
	public String[] getDescription() {
		return new String[] { "This is an empty component which performs no operation. This component is created to support external mappings." };
	}

	@Override
	public void addConfigDescriptions() throws InvalidParameter {
		// TODO Auto-generated method stub

	}

}
