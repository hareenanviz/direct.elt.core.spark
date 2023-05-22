package com.anvizent.elt.core.spark.source.config.bean;

import java.io.Serializable;

import org.apache.spark.sql.types.StructType;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public interface CoreSource extends Serializable {

	StructType getStructType();
}
