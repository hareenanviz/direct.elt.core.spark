package com.anvizent.elt.core.spark.util;

import java.io.Serializable;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class MutableInteger implements Serializable {
	private static final long serialVersionUID = 1L;

	private int i;

	public MutableInteger() {
	}

	public MutableInteger(int i) {
		this.i = i;
	}

	public int get() {
		return i;
	}

	public MutableInteger add(int i) {
		this.i += i;
		return this;
	}

	public MutableInteger subtract(int i) {
		this.i -= i;
		return this;
	}

	public void set(int i) {
		this.i = i;
	}

	@Override
	public String toString() {
		return i + "";
	}
}
