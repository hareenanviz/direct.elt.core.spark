package com.anvizent.elt.core.spark.common.util;

/**
 * @author Hareen Bejjanki
 *
 */
public class NaturalNumber {

	private long n;

	public NaturalNumber(long n) {
		this.n = n;
	}

	public long set(long n) {
		long oldValue = this.n;
		this.n = n;
		if (this.n < 0) {
			this.n = 0;
		}

		return oldValue;
	}

	public long get() {
		if (n < 0) {
			n = 0;
		}

		return n;
	}

	public NaturalNumber increament() {
		n++;
		if (n < 0) {
			n = 0;
		}

		return this;
	}

	public NaturalNumber decreament() {
		n--;
		if (n < 0) {
			n = 0;
		}

		return this;
	}

	public NaturalNumber add(long n) {
		this.n += n;
		if (this.n < 0) {
			this.n = 0;
		}

		return this;
	}

	public NaturalNumber add(NaturalNumber n) {
		this.n += n.n;
		if (this.n < 0) {
			this.n = 0;
		}

		return this;
	}

	public NaturalNumber subtract(long n) {
		this.n -= n;
		if (this.n < 0) {
			this.n = 0;
		}

		return this;
	}

	public NaturalNumber subtract(NaturalNumber n) {
		this.n -= n.n;
		if (this.n < 0) {
			this.n = 0;
		}

		return this;
	}
}
