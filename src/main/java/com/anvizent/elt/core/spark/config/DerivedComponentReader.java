package com.anvizent.elt.core.spark.config;

import java.io.Serializable;

import com.anvizent.elt.core.spark.exception.ConfigReferenceNotFoundException;
import com.anvizent.elt.core.spark.exception.InvalidInputForConfigException;
import com.anvizent.elt.core.spark.util.MutableInteger;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public abstract class DerivedComponentReader implements Serializable {

	private static final long serialVersionUID = 1L;

	protected String componentName;
	protected String source;
	protected final MutableInteger seek = new MutableInteger();

	public String getComponentName() {
		return componentName;
	}

	public MutableInteger getSeek() {
		return seek;
	}

	public abstract DerivedComponentReader reset();

	public abstract DerivedComponentReader setConfigSource(String configSource) throws ConfigurationReadingException, InvalidInputForConfigException;

	public abstract String getSeekName();

	public abstract ComponentConfiguration readNextConfiguration()
			throws ConfigurationReadingException, InvalidInputForConfigException, ConfigReferenceNotFoundException;
}
