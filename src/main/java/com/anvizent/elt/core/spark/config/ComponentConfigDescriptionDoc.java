package com.anvizent.elt.core.spark.config;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.anvizent.elt.core.lib.exception.RuntimeInvalidArgumentException;

/**
 * @author Hareen Bejjanki
 *
 */
public class ComponentConfigDescriptionDoc implements Serializable {
	private static final long serialVersionUID = 1L;

	private String element;
	private Map<String, String> attributes = new HashMap<>();
	private List<ComponentConfigDescriptionDoc> children = new LinkedList<>();
	private boolean textElement;

	public String getElement() {
		return element;
	}

	public Map<String, String> getAttributes() {
		return attributes;
	}

	public ComponentConfigDescriptionDoc setAttributes(Map<String, String> attributes) {
		if (textElement && attributes != null && attributes.size() != 0) {
			throw new RuntimeInvalidArgumentException("Attributes are not supported for textElement");
		}

		this.attributes = attributes;
		return this;
	}

	public ComponentConfigDescriptionDoc putAttribute(String attributeKey, String attributeValue) {
		if (textElement) {
			throw new RuntimeInvalidArgumentException("Attributes are not supported for textElement");
		}

		this.attributes.put(attributeKey, attributeValue);
		return this;
	}

	public List<ComponentConfigDescriptionDoc> getChildren() {
		return children;
	}

	public ComponentConfigDescriptionDoc setChildren(List<ComponentConfigDescriptionDoc> children) {
		if (textElement && children != null && children.size() != 0) {
			throw new RuntimeInvalidArgumentException("Childrens are not supported for textElement");
		}

		this.children = children;
		return this;
	}

	public ComponentConfigDescriptionDoc addChild(ComponentConfigDescriptionDoc child) {
		if (textElement) {
			throw new RuntimeInvalidArgumentException("Childrens are not supported for textElement");
		}

		this.children.add(child);
		return this;
	}

	public boolean isTextElement() {
		return textElement;
	}

	public ComponentConfigDescriptionDoc(String element, boolean textElement) {
		this.element = element;
		this.textElement = textElement;
	}
}
