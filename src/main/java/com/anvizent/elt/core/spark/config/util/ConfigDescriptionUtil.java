package com.anvizent.elt.core.spark.config.util;

import com.anvizent.elt.core.spark.config.ComponentConfigDoc;
import com.anvizent.elt.core.spark.constant.HTMLTextStyle;
import com.anvizent.elt.core.spark.constant.HelpConstants;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.util.IndexedAndLinkedMap;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ConfigDescriptionUtil {

	private IndexedAndLinkedMap<String, ComponentConfigDoc> configDescriptions = new IndexedAndLinkedMap<>();

	public IndexedAndLinkedMap<String, ComponentConfigDoc> getConfigDescriptions() {
		return configDescriptions;
	}

	public void addConfigDescription(String name, String mandatory, String[] description) throws InvalidParameter {
		addConfigDescription(name, mandatory, null, description, null, null, false, null);

	}

	public void addConfigDescription(String name, String mandatory, String[] description, boolean ignoreFirstDescription) throws InvalidParameter {
		addConfigDescription(name, mandatory, null, description, null, null, ignoreFirstDescription, null);
	}

	public void addConfigDescription(String name, String mandatory, String[] description, boolean ignoreFirstDescription, HTMLTextStyle descriptionStyle)
	        throws InvalidParameter {
		addConfigDescription(name, mandatory, null, description, null, null, ignoreFirstDescription, descriptionStyle);

	}

	public void addConfigDescription(String name, String mandatory, String defaultValue, String[] description) throws InvalidParameter {
		addConfigDescription(name, mandatory, defaultValue, description, null, null, false, null);
	}

	public void addConfigDescription(String name, String mandatory, String defaultValue, String[] description, boolean ignoreFirstDescription)
	        throws InvalidParameter {
		addConfigDescription(name, mandatory, defaultValue, description, null, null, ignoreFirstDescription, null);
	}

	public void addConfigDescription(String name, String mandatory, String defaultValue, String[] description, boolean ignoreFirstDescription,
	        HTMLTextStyle descriptionStyle) throws InvalidParameter {
		addConfigDescription(name, mandatory, defaultValue, description, null, null, ignoreFirstDescription, descriptionStyle);
	}

	public void addConfigDescription(String name, String mandatory, String defaultValue, String[] description, boolean ignoreFirstDescription,
	        HTMLTextStyle descriptionStyle, String metaSimilarTo, String type) throws InvalidParameter {
		addConfigDescription(name, mandatory, defaultValue, description, metaSimilarTo, type, ignoreFirstDescription, descriptionStyle);
	}

	public void addConfigDescription(String name, String mandatory, String defaultValue, String[] description, String metaSimilarTo) throws InvalidParameter {
		addConfigDescription(name, mandatory, defaultValue, description, metaSimilarTo, null, false, null);

	}

	public void addConfigDescription(String name, String mandatory, String defaultValue, String[] description, String metaSimilarTo,
	        boolean ignoreFirstDescription) throws InvalidParameter {
		addConfigDescription(name, mandatory, defaultValue, description, metaSimilarTo, null, ignoreFirstDescription, null);
	}

	public void addConfigDescription(String name, String mandatory, String defaultValue, String[] description, String metaSimilarTo,
	        boolean ignoreFirstDescription, HTMLTextStyle descriptionStyle) throws InvalidParameter {
		addConfigDescription(name, mandatory, defaultValue, description, metaSimilarTo, HelpConstants.Type.STRING, ignoreFirstDescription, descriptionStyle);
	}

	public void addConfigDescription(String name, String mandatory, String[] description, String type) throws InvalidParameter {
		addConfigDescription(name, mandatory, null, description, null, type, false, null);

	}

	public void addConfigDescription(String name, String mandatory, String[] description, String type, boolean ignoreFirstDescription) throws InvalidParameter {
		addConfigDescription(name, mandatory, null, description, null, type, ignoreFirstDescription, null);
	}

	public void addConfigDescription(String name, String mandatory, String[] description, String type, boolean ignoreFirstDescription,
	        HTMLTextStyle descriptionStyle) throws InvalidParameter {
		addConfigDescription(name, mandatory, null, description, null, type, ignoreFirstDescription, descriptionStyle);
	}

	public void addConfigDescription(String name, String mandatory, String defaultValue, String[] description, String metaSimilarTo, String type)
	        throws InvalidParameter {
		addConfigDescription(name, mandatory, defaultValue, description, metaSimilarTo, type, false, null);
	}

	public void addConfigDescription(String name, String mandatory, String defaultValue, String[] description, String metaSimilarTo, String type,
	        boolean ignoreFirstDescription) throws InvalidParameter {
		addConfigDescription(name, mandatory, defaultValue, description, metaSimilarTo, type, ignoreFirstDescription, null);

	}

	public void addConfigDescription(String name, String mandatory, String defaultValue, String[] description, String meataSimilarTo, String type,
	        boolean ignoreFirstDescription, HTMLTextStyle descriptionStyle) throws InvalidParameter {
		ComponentConfigDoc configDescription = new ComponentConfigDoc(name, mandatory, defaultValue, description, meataSimilarTo,
		        (type == null || type.isEmpty() ? HelpConstants.Type.STRING : type), ignoreFirstDescription,
		        descriptionStyle == null ? HTMLTextStyle.NONE : descriptionStyle);
		configDescriptions.putLast(name, configDescription);
	}

}