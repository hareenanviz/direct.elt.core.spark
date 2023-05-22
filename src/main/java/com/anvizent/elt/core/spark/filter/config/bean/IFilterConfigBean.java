package com.anvizent.elt.core.spark.filter.config.bean;

import java.util.ArrayList;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public interface IFilterConfigBean {

	public ArrayList<String> getEmitStreamNames();

	public void setEmitStreamNames(ArrayList<String> emitStreamNames);
}
