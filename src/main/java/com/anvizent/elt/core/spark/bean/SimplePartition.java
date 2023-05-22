/**
 * 
 */
package com.anvizent.elt.core.spark.bean;

import org.apache.spark.Partition;

/**
 * @author Hareen Bejjanki
 *
 */
public class SimplePartition implements Partition {

	private static final long serialVersionUID = 1L;
	private int index;

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}

	public SimplePartition(int index) {
		this.index = index;
	}

	@Override
	public int index() {
		return this.index;
	}

}
