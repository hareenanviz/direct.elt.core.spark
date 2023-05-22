package com.anvizent.elt.core.spark.doc.helper;

import com.anvizent.elt.core.spark.config.ComponentConfigDoc;
import com.anvizent.elt.core.spark.config.util.ConfigDescriptionUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.factory.RetriableFactory;
import com.anvizent.elt.core.spark.factory.RetryMandatoryFactory;
import com.anvizent.elt.core.spark.operation.factory.MultiInputOperationFactory;
import com.anvizent.elt.core.spark.source.factory.SourceFactory;
import com.anvizent.elt.core.spark.util.IndexedAndLinkedMap;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public abstract class DocHelper {

	private Factory factory;

	protected ConfigDescriptionUtil configDescriptionUtil = new ConfigDescriptionUtil();

	public IndexedAndLinkedMap<String, ComponentConfigDoc> getConfigDescriptions() {
		return configDescriptionUtil.getConfigDescriptions();
	}

	public DocHelper(Factory factory) throws InvalidParameter {
		this.factory = factory;
		initConfigDescriptions();
	}

	private void initConfigDescriptions() throws InvalidParameter {
		configDescriptionUtil.addConfigDescription(General.NAME, General.YES, "", new String[] { "Name of the component." });

		if (!(factory instanceof SourceFactory)) {
			if (!(factory instanceof MultiInputOperationFactory)) {
				configDescriptionUtil.addConfigDescription(General.SOURCE, General.YES, "",
				        new String[] { "Source component names." + "Minumum number of sources for this component are '" + factory.getMinInputs()
				                + "' and maximum inputs are '" + factory.getMaxInputs() + "'" });
			} else {
				configDescriptionUtil.addConfigDescription(General.SOURCE, General.YES, "", new String[] { "Source component name." });
			}
		}

		configDescriptionUtil.addConfigDescription(General.PERSIST, General.NO, Boolean.FALSE.toString(),
		        new String[] { "If true the component and its inner mapping components are persisted using Spark caching."
		                + "If the current component is used for more than one component as a source, its recomended to make this value as true to avoid recomputation of this component and the components before." });

		if (factory instanceof RetriableFactory || factory instanceof RetryMandatoryFactory) {
			configDescriptionUtil.addConfigDescription(General.MAX_RETRY_COUNT, General.NO, "1",
			        new String[] { "For the given value(a positive number) component will retry for failed record/batch." });
			configDescriptionUtil.addConfigDescription(General.RETRY_DELAY, General.NO, "0",
			        new String[] { "if there is a retry for failed record/batch this config value defines the time gap between the retries."
			                + "The value is in milli seconds." });
		}

		addConfigDescriptions();
	}

	public abstract String[] getDescription();

	public abstract void addConfigDescriptions() throws InvalidParameter;

	public String getName() {
		return factory.getName();
	}
}