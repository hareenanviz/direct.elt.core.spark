package com.anvizent.elt.core.spark.config;

import java.io.Serializable;

import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.Constants.ExceptionMessage;
import com.anvizent.elt.core.spark.constant.HTMLTextStyle;
import com.anvizent.elt.core.spark.exception.InvalidParameter;

/**
 * @author Hareen Bejjanki
 *
 */
public class ComponentConfigDoc implements Serializable {
	private static final long serialVersionUID = 1L;

	private String name;
	private String mandatory;
	private String defaultValue;
	private ComponentConfigDescriptionDoc descriptionDoc;
	private String meataSimilarTo;
	private ComponentConfigDescriptionDoc meataSimilarToDoc;
	private boolean ignoreFirstDescription;
	private HTMLTextStyle htmlTextStyle;
	private String type;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getMandatory() {
		return mandatory;
	}

	public void setMandatory(String mandatory) {
		this.mandatory = mandatory;
	}

	public String getDefaultValue() {
		return defaultValue;
	}

	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}

	public ComponentConfigDescriptionDoc getDescriptionDoc() {
		return descriptionDoc;
	}

	public ComponentConfigDoc setDescriptionDoc(ComponentConfigDescriptionDoc descriptionDoc) {
		this.descriptionDoc = descriptionDoc;
		return this;
	}

	public String getMeataSimilarTo() {
		return meataSimilarTo;
	}

	public void setMeataSimilarTo(String meataSimilarTo) {
		this.meataSimilarTo = meataSimilarTo;
	}

	public boolean canIgnoreFirstDescription() {
		return ignoreFirstDescription;
	}

	public void setIgnoreFirstDescription(boolean ignoreFirstDescription) {
		this.ignoreFirstDescription = ignoreFirstDescription;
	}

	public HTMLTextStyle getHtmlTextStyle() {
		return htmlTextStyle;
	}

	public void setDescriptionStyle(HTMLTextStyle htmlTextStyle) {
		this.htmlTextStyle = htmlTextStyle;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public ComponentConfigDoc(String name, String mandatory, String defaultValue, String[] description, String meataSimilarTo, String type,
	        boolean ignoreFirstDescription, HTMLTextStyle htmlTextStyle) throws InvalidParameter {
		validate(name, mandatory, defaultValue, description);
		this.name = name;
		this.mandatory = mandatory;
		this.defaultValue = defaultValue;
		this.descriptionDoc = new ComponentConfigDescriptionDoc("div", false);

		if (description != null && description.length > 0) {
			for (String descriptionElem : description) {
				descriptionDoc.addChild(new ComponentConfigDescriptionDoc("p", false).addChild(new ComponentConfigDescriptionDoc(descriptionElem, true)));
			}
		}

		this.meataSimilarTo = meataSimilarTo;
		this.type = type;
		this.ignoreFirstDescription = ignoreFirstDescription;
		this.htmlTextStyle = htmlTextStyle;
	}

	public ComponentConfigDoc(String name, String mandatory, String defaultValue, String type) throws InvalidParameter {
		validate(name, mandatory, defaultValue);
		this.name = name;
		this.mandatory = mandatory;
		this.defaultValue = defaultValue;
		this.type = type;
	}

	private void validate(String name, String mandatory, String defaultValue, String[] description) throws InvalidParameter {
		if (name == null || name.isEmpty()) {
			throw new InvalidParameter(ExceptionMessage.NAME, true);
		}

		if (mandatory == null || mandatory.isEmpty()) {
			throw new InvalidParameter(ExceptionMessage.MANDATORY, true);
		}

		if (mandatory.equals(General.YES) && defaultValue != null && !defaultValue.isEmpty()) {
			throw new InvalidParameter(ExceptionMessage.CANNOT_PROVIDE_DEFAULT_VALUE_FOR_MANDATORY_CONFIG, false);
		}

		if (description == null || description.length == 0) {
			throw new InvalidParameter(ExceptionMessage.DESCRIPTION, true);
		}
	}

	private void validate(String name, String mandatory, String defaultValue) throws InvalidParameter {
		if (name == null || name.isEmpty()) {
			throw new InvalidParameter(ExceptionMessage.NAME, true);
		}

		if (mandatory == null || mandatory.isEmpty()) {
			throw new InvalidParameter(ExceptionMessage.MANDATORY, true);
		}

		if (mandatory.equals(General.YES) && defaultValue != null && !defaultValue.isEmpty()) {
			throw new InvalidParameter(ExceptionMessage.CANNOT_PROVIDE_DEFAULT_VALUE_FOR_MANDATORY_CONFIG, false);
		}
	}
}
