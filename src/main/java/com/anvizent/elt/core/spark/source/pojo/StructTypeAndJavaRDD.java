package com.anvizent.elt.core.spark.source.pojo;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.types.StructType;

public class StructTypeAndJavaRDD<T> {

	private JavaRDD<T> rdd;
	private StructType structType;

	public JavaRDD<T> getRdd() {
		return rdd;
	}

	public void setRdd(JavaRDD<T> rdd) {
		this.rdd = rdd;
	}

	public StructType getStructType() {
		return structType;
	}

	public void setStructType(StructType structType) {
		this.structType = structType;
	}

	public StructTypeAndJavaRDD(JavaRDD<T> rdd, StructType structType) {
		this.rdd = rdd;
		this.structType = structType;
	}

}
