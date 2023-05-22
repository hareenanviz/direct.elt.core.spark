package com.anvizent.elt.core.spark.source.config.bean;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class PartitionConfigBean implements Serializable {

	private static final long serialVersionUID = 1L;

	private ArrayList<String> partitionColumns;
	private BigDecimal lowerBound;
	private BigDecimal upperBound;
	private Integer numberOfPartitions;

	public ArrayList<String> getPartitionColumns() {
		return partitionColumns;
	}

	public void setPartitionColumns(ArrayList<String> partitionColumns) {
		this.partitionColumns = partitionColumns;
	}

	public BigDecimal getLowerBound() {
		return lowerBound;
	}

	public void setLowerBound(BigDecimal lowerBound) {
		this.lowerBound = lowerBound;
	}

	public BigDecimal getUpperBound() {
		return upperBound;
	}

	public void setUpperBound(BigDecimal upperBound) {
		this.upperBound = upperBound;
	}

	public Integer getNumberOfPartitions() {
		return numberOfPartitions;
	}

	public void setNumberOfPartitions(Integer numberOfPartitions) {
		this.numberOfPartitions = numberOfPartitions;
	}
}
