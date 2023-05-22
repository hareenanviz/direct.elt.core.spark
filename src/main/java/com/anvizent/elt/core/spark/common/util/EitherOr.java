package com.anvizent.elt.core.spark.common.util;

/**
 * @author Hareen Bejjanki
 *
 * @param <A>
 * @param <B>
 */
public class EitherOr<A, B> {

	private A a;
	private B b;

	public A getA() {
		return a;
	}

	public void setA(A a) {
		this.a = a;
		this.b = null;
	}

	public B getB() {
		return b;
	}

	public void setB(B b) {
		this.b = b;
		this.a = null;
	}

}
