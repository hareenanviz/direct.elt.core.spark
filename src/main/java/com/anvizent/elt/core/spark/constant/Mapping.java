package com.anvizent.elt.core.spark.constant;

import com.anvizent.elt.core.spark.constant.ConfigConstants.Mapping.Description;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Mapping.General;
import com.anvizent.elt.core.spark.mapping.factory.CoerceFactory;
import com.anvizent.elt.core.spark.mapping.factory.ConditionalReplacementCleansingFactory;
import com.anvizent.elt.core.spark.mapping.factory.ConstantFactory;
import com.anvizent.elt.core.spark.mapping.factory.DateAndTimeGranularityFactory;
import com.anvizent.elt.core.spark.mapping.factory.DuplicateFactory;
import com.anvizent.elt.core.spark.mapping.factory.MappingFactory;
import com.anvizent.elt.core.spark.mapping.factory.RenameFactory;
import com.anvizent.elt.core.spark.mapping.factory.ReplicateFactory;
import com.anvizent.elt.core.spark.mapping.factory.RepositionFactory;
import com.anvizent.elt.core.spark.mapping.factory.RetainFactory;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public enum Mapping {

	CONDITIONAL_REPLACEMENT_CLEANSING(General.MAPPING + Constants.General.KEY_SEPARATOR + General.CONDITIONAL_REPLACEMENT_CLEANSING,
			new ConditionalReplacementCleansingFactory(), new String[] { Description.CONDITIONAL_REPLACEMENT_CLEANSING }), // .
	DUPLICATE(General.MAPPING + Constants.General.KEY_SEPARATOR + General.DUPLICATE, new DuplicateFactory(), new String[] { Description.DUPLICATE }), // .
	REPLICATE(General.MAPPING + Constants.General.KEY_SEPARATOR + General.REPLICATE, new ReplicateFactory(), new String[] { Description.REPLICATE }), // .
	CONSTANTS(General.MAPPING + Constants.General.KEY_SEPARATOR + General.CONSTANT, new ConstantFactory(), new String[] { Description.CONSTANT }), // .
	DATE_AND_TIME_GRANULARITY(General.MAPPING + Constants.General.KEY_SEPARATOR + General.DATE_AND_TIME_GRANULARITY, new DateAndTimeGranularityFactory(),
			new String[] { Description.DATE_AND_TIME_GRANULARITY }), // .
	REPOSITION(General.MAPPING + Constants.General.KEY_SEPARATOR + General.REPOSITION, new RepositionFactory(), new String[] { Description.REPOSITION }), // .
	COERCE(General.MAPPING + Constants.General.KEY_SEPARATOR + General.COERCE, new CoerceFactory(), new String[] { Description.COERCE }), // .
	RENAME(General.MAPPING + Constants.General.KEY_SEPARATOR + General.RENAME, new RenameFactory(), new String[] { Description.RENAME }), // .
	RETAIN(General.MAPPING + Constants.General.KEY_SEPARATOR + General.RETAIN, new RetainFactory(), new String[] { Description.RETAIN });

	private String mappingName;
	private MappingFactory mappingFactory;
	private String[] description;

	public String getMappingName() {
		return mappingName;
	}

	public MappingFactory getMappingFactory() {
		return mappingFactory;
	}

	public String[] getDescription() {
		return description;
	}

	private Mapping(String mappingName, MappingFactory mappingFactory, String[] description) {
		this.mappingName = mappingName;
		this.mappingFactory = mappingFactory;
		this.description = description;
	}

	public static Mapping getInstance(String configName) {
		return getInstance(configName, null);
	}

	public static Mapping getInstance(String configName, Mapping defaultValue) {
		if (configName == null || configName.isEmpty()) {
			return defaultValue;
		} else if (configName.equalsIgnoreCase(COERCE.mappingName)) {
			return COERCE;
		} else if (configName.equalsIgnoreCase(CONSTANTS.mappingName)) {
			return CONSTANTS;
		} else if (configName.equalsIgnoreCase(DUPLICATE.mappingName)) {
			return DUPLICATE;
		} else if (configName.equalsIgnoreCase(RENAME.mappingName)) {
			return RENAME;
		} else if (configName.equalsIgnoreCase(REPLICATE.mappingName)) {
			return REPLICATE;
		} else if (configName.equalsIgnoreCase(RETAIN.mappingName)) {
			return RETAIN;
		} else if (configName.equalsIgnoreCase(CONDITIONAL_REPLACEMENT_CLEANSING.mappingName)) {
			return CONDITIONAL_REPLACEMENT_CLEANSING;
		} else if (configName.equalsIgnoreCase(DATE_AND_TIME_GRANULARITY.mappingName)) {
			return DATE_AND_TIME_GRANULARITY;
		} else if (configName.equalsIgnoreCase(REPOSITION.mappingName)) {
			return REPOSITION;
		} else {
			return null;
		}
	}
}
