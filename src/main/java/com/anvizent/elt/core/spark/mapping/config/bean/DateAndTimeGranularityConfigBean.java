package com.anvizent.elt.core.spark.mapping.config.bean;

import java.util.ArrayList;

import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;
import com.anvizent.elt.core.spark.constant.Granularity;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class DateAndTimeGranularityConfigBean extends MappingConfigBean {

	private static final long serialVersionUID = 1L;

	private ArrayList<String> fields;
	private ArrayList<Granularity> granularities;
	private boolean allDateFields;

	public ArrayList<String> getFields() {
		return fields;
	}

	public void setFields(ArrayList<String> fields) {
		this.fields = fields;
	}

	public ArrayList<Granularity> getGranularities() {
		return granularities;
	}

	public void setGranularities(ArrayList<Granularity> granularities) {
		this.granularities = granularities;
	}

	public boolean isAllDateFields() {
		return allDateFields;
	}

	public void setAllDateFields(boolean allDateFields) {
		this.allDateFields = allDateFields;
	}

	public DateAndTimeGranularityConfigBean(ArrayList<String> fields, ArrayList<Granularity> granularities, boolean allDateFields) {
		this.fields = fields;
		this.granularities = granularities;
		this.allDateFields = allDateFields;
	}

}
