package com.anvizent.elt.core.spark.constant;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public enum SQLTypes {

	BIT(-7), TINYINT(-6), SMALLINT(5), INTEGER(4), BIGINT(-5), FLOAT(6), REAL(7), DOUBLE(8), NUMERIC(2), DECIMAL(3), CHAR(1), VARCHAR(12), LONGVARCHAR(-1),
	BINARY(-2), VARBINARY(-3), LONGVARBINARY(-4), NCHAR(-15), NVARCHAR(-9), LONGNVARCHAR(-16), DATE(91), TIME(92), TIMESTAMP(93);

	private Integer value;

	private SQLTypes(Integer value) {
		this.value = value;
	}

	public static SQLTypes getInstance(Integer value) {
		return getInstance(value, DATE);
	}

	private static SQLTypes getInstance(Integer value, SQLTypes defaultValue) {
		if (value == null) {
			return defaultValue;
		} else if (value.equals(BIT.value)) {
			return BIT;
		} else if (value.equals(TINYINT.value)) {
			return TINYINT;
		} else if (value.equals(SMALLINT.value)) {
			return SMALLINT;
		} else if (value.equals(INTEGER.value)) {
			return INTEGER;
		} else if (value.equals(BIGINT.value)) {
			return BIGINT;
		} else if (value.equals(FLOAT.value)) {
			return FLOAT;
		} else if (value.equals(REAL.value)) {
			return REAL;
		} else if (value.equals(DOUBLE.value)) {
			return DOUBLE;
		} else if (value.equals(NUMERIC.value)) {
			return NUMERIC;
		} else if (value.equals(DECIMAL.value)) {
			return DECIMAL;
		} else if (value.equals(CHAR.value)) {
			return CHAR;
		} else if (value.equals(VARCHAR.value)) {
			return VARCHAR;
		} else if (value.equals(LONGVARCHAR.value)) {
			return LONGVARCHAR;
		} else if (value.equals(BINARY.value)) {
			return BINARY;
		} else if (value.equals(VARBINARY.value)) {
			return VARBINARY;
		} else if (value.equals(LONGVARBINARY.value)) {
			return LONGVARBINARY;
		} else if (value.equals(NCHAR.value)) {
			return NCHAR;
		} else if (value.equals(NVARCHAR.value)) {
			return NVARCHAR;
		} else if (value.equals(LONGNVARCHAR.value)) {
			return LONGNVARCHAR;
		} else if (value.equals(DATE.value)) {
			return DATE;
		} else if (value.equals(TIME.value)) {
			return TIME;
		} else if (value.equals(TIMESTAMP.value)) {
			return TIMESTAMP;
		} else {
			return null;
		}
	}
}
