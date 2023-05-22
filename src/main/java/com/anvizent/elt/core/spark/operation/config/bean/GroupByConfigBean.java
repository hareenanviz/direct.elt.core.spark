package com.anvizent.elt.core.spark.operation.config.bean;

import java.util.ArrayList;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.spark.constant.Aggregations;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class GroupByConfigBean extends ConfigBean implements SimpleOperationConfigBean {

	private static final long serialVersionUID = 1L;

	private ArrayList<String> groupByFields;
	private ArrayList<Integer> groupByFieldPositions;
	private ArrayList<Aggregations> aggregations;
	private ArrayList<String> joinAggregationDelimeters;
	private ArrayList<String> aggregationFields;
	private ArrayList<String> aliasNames;
	private ArrayList<Integer> aggregationFieldPositions;
	private ArrayList<Integer> precisions;
	private ArrayList<Integer> scales;

	public ArrayList<String> getGroupByFields() {
		return groupByFields;
	}

	public void setGroupByFields(ArrayList<String> groupByFields) {
		this.groupByFields = groupByFields;
	}

	public ArrayList<Integer> getGroupByFieldPositions() {
		return groupByFieldPositions;
	}

	public void setGroupByFieldPositions(ArrayList<Integer> groupByFieldPositions) {
		this.groupByFieldPositions = groupByFieldPositions;
	}

	public ArrayList<Aggregations> getAggregations() {
		return aggregations;
	}

	public void setAggregations(ArrayList<Aggregations> aggregations) {
		this.aggregations = aggregations;
	}

	public ArrayList<String> getJoinAggregationDelimeters() {
		return joinAggregationDelimeters;
	}

	public void setJoinAggregationsDelimeter(ArrayList<String> joinAggregationDelimeters) {
		this.joinAggregationDelimeters = joinAggregationDelimeters;
	}

	public ArrayList<String> getAggregationFields() {
		return aggregationFields;
	}

	public void setAggregationFields(ArrayList<String> aggregationFields) {
		this.aggregationFields = aggregationFields;
	}

	public ArrayList<String> getAliasNames() {
		return aliasNames;
	}

	public void setAliasNames(ArrayList<String> aliasNames) {
		this.aliasNames = aliasNames;
	}

	public ArrayList<Integer> getAggregationFieldPositions() {
		return aggregationFieldPositions;
	}

	public void setAggregationFieldPositions(ArrayList<Integer> aggregationFieldPositions) {
		this.aggregationFieldPositions = aggregationFieldPositions;
	}

	public ArrayList<Integer> getPrecisions() {
		return precisions;
	}

	public void setPrecisions(ArrayList<Integer> precisions) {
		this.precisions = precisions;
	}

	public ArrayList<Integer> getScales() {
		return scales;
	}

	public void setScales(ArrayList<Integer> scales) {
		this.scales = scales;
	}

}
