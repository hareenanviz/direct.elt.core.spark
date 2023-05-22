package com.anvizent.elt.core.spark.operation.service;

import java.math.BigDecimal;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;

import com.anvizent.elt.core.spark.constant.Constants.ExceptionMessage;
import com.anvizent.elt.core.spark.exception.AggregationException;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class AggregationService {

	public static Object sum(Object row_1Value, Object row_2Value) throws AggregationException {
		if (row_1Value != null) {
			Class type = row_1Value.getClass();
			if (type.equals(Integer.class)) {
				return (Integer) row_1Value + (row_2Value == null ? 0 : (Integer) row_2Value);
			} else if (type.equals(Float.class)) {
				return (Float) row_1Value + (row_2Value == null ? 0 : (Float) row_2Value);
			} else if (type.equals(Double.class)) {
				return (Double) row_1Value + (row_2Value == null ? 0 : (Double) row_2Value);
			} else if (type.equals(Long.class)) {
				return (Long) row_1Value + (row_2Value == null ? 0 : (Long) row_2Value);
			} else if (type.equals(Byte.class)) {
				return (Byte) row_1Value + (row_2Value == null ? 0 : (Byte) row_2Value);
			} else if (type.equals(Short.class)) {
				return (Short) row_1Value + (row_2Value == null ? 0 : (Short) row_2Value);
			} else if (type.equals(BigDecimal.class)) {
				return ((BigDecimal) row_1Value).add(row_2Value == null ? new BigDecimal(0) : (BigDecimal) row_2Value);
			} else {
				throw new AggregationException(
				        MessageFormat.format(ExceptionMessage.CANNOT_PERFORM_SUM_ON_VALUE_OF_TYPE, row_1Value.toString(), type.toString()));
			}
		} else {
			return row_2Value;
		}
	}

	public static Object count(Object row_1Value, Object row_2Value) {
		return (Long) row_1Value + (row_2Value == null ? 0 : 1);
	}

	public static Object combinerCount(Object row_1Value, Object row_2Value) {
		return (Long) row_1Value + (Long) row_2Value;
	}

	public static Object countWithNulls(Object row_1Value) {
		return (Long) row_1Value + 1;
	}

	public static Object combinerCountWithNulls(Object row_1Value, Object row_2Value) {
		return (Long) row_1Value + (Long) row_2Value;
	}

	public static Object min(Object row_1Value, Object row_2Value) throws AggregationException {
		ArrayList minElements = new ArrayList();

		if (row_1Value != null) {
			minElements.add(row_1Value);
		}
		if (row_2Value != null) {
			minElements.add(row_2Value);
		}

		try {
			return minElements.isEmpty() ? null : Collections.min(minElements);
		} catch (Exception exception) {
			throw new AggregationException(ExceptionMessage.ERROR_WHILE_PERFORMING_MIN_OPERATION + minElements);
		}
	}

	public static Object max(Object row_1Value, Object row_2Value) throws AggregationException {
		ArrayList maxElements = new ArrayList();

		if (row_1Value != null) {
			maxElements.add(row_1Value);
		}
		if (row_2Value != null) {
			maxElements.add(row_2Value);
		}

		try {
			return maxElements.isEmpty() ? null : Collections.max(maxElements);
		} catch (Exception exception) {
			throw new AggregationException(ExceptionMessage.ERROR_WHILE_PERFORMING_MAX_OPERATION + maxElements);
		}
	}

	public static Object average(Object sumValue, Object countValue) throws AggregationException {
		if ((Long) countValue == 0) {
			return new BigDecimal(0);
		} else if (sumValue != null) {
			Class type = sumValue.getClass();

			if (type.equals(Integer.class)) {
				return new BigDecimal((Integer) sumValue / (Long) countValue);
			} else if (type.equals(Float.class)) {
				return new BigDecimal((Float) sumValue / (Long) countValue);
			} else if (type.equals(Double.class)) {
				return new BigDecimal((Double) sumValue / (Long) countValue);
			} else if (type.equals(Long.class)) {
				return new BigDecimal((Long) sumValue / (Long) countValue);
			} else if (type.equals(Byte.class)) {
				return new BigDecimal((Byte) sumValue / (Long) countValue);
			} else if (type.equals(Short.class)) {
				return new BigDecimal((Short) sumValue / (Long) countValue);
			} else if (type.equals(BigDecimal.class)) {
				return ((BigDecimal) sumValue).divide(new BigDecimal((Long) countValue));
			} else {
				throw new AggregationException(
				        MessageFormat.format(ExceptionMessage.CANNOT_PERFORM_AVERAGE_ON_VALUE_OF_TYPE, sumValue.toString(), type.toString()));
			}
		} else {
			return sumValue;
		}
	}

	public static Object getSumZeroValue(String aggregationField, Class javaType) throws AggregationException {
		if (javaType.equals(Integer.class)) {
			return 0;
		} else if (javaType.equals(Long.class)) {
			return 0L;
		} else if (javaType.equals(Float.class)) {
			return 0F;
		} else if (javaType.equals(Double.class)) {
			return 0D;
		} else if (javaType.equals(Byte.class)) {
			return (byte) 0;
		} else if (javaType.equals(Short.class)) {
			return (short) 0;
		} else if (javaType.equals(BigDecimal.class)) {
			return new BigDecimal(0);
		} else {
			throw new AggregationException(aggregationField, javaType);
		}
	}

	public static Object getMinZeroValue(String aggregationField, Class javaType) throws AggregationException {
		if (javaType.equals(Integer.class)) {
			return Integer.MAX_VALUE;
		} else if (javaType.equals(Long.class)) {
			return Long.MAX_VALUE;
		} else if (javaType.equals(Float.class)) {
			return Float.MAX_VALUE;
		} else if (javaType.equals(Double.class)) {
			return Double.MAX_VALUE;
		} else if (javaType.equals(Byte.class)) {
			return Byte.MAX_VALUE;
		} else if (javaType.equals(Short.class)) {
			return Short.MAX_VALUE;
		} else if (javaType.equals(String.class)) {
			return null;
		} else if ((javaType.equals(Date.class) || javaType.equals(java.sql.Date.class))) {
			// new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("9999-12-31
			// 23:59:59")).getTime()
			return new Date(253402280999000l);
		} else if (javaType.equals(BigDecimal.class)) {
			// TODO
			return new BigDecimal(Double.MAX_VALUE);
		} else {
			throw new AggregationException(aggregationField, javaType);
		}
	}

	public static Object getMaxZeroValue(String aggregationField, Class javaType) throws AggregationException {
		if (javaType.equals(Integer.class)) {
			return Integer.MIN_VALUE;
		} else if (javaType.equals(Long.class)) {
			return Long.MIN_VALUE;
		} else if (javaType.equals(Float.class)) {
			return 1D - Float.MAX_VALUE;
		} else if (javaType.equals(Double.class)) {
			return 1D - Double.MAX_VALUE;
		} else if (javaType.equals(Byte.class)) {
			return Byte.MIN_VALUE;
		} else if (javaType.equals(Short.class)) {
			return Short.MIN_VALUE;
		} else if (javaType.equals(String.class) || javaType.equals(Character.class)) {
			return null;
		} else if ((javaType.equals(Date.class) || javaType.equals(java.sql.Date.class))) {
			// new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("0000-01-01
			// 00:00:00")).getTime()
			return new Date(-62167411800000l);
		} else if (javaType.equals(BigDecimal.class)) {
			// TODO
			return new BigDecimal(1D - Double.MAX_VALUE);
		} else {
			throw new AggregationException(aggregationField, javaType);
		}
	}

	public static Object random(Object value1, Object value2) {
		if (value1 == null && value2 == null) {
			return null;
		} else if (value1 == null) {
			return value2;
		} else if (value2 == null) {
			return value1;
		} else {
			return ((int) (Math.random() * 2)) == 0 ? value1 : value2;
		}
	}

	public static String joinByDelim(Object value1, Object value2, String delim) {
		if (value1 == null && value2 == null) {
			return null;
		} else if (value1 == null) {
			return value2.toString();
		} else if (value2 == null) {
			return value1.toString();
		} else {
			return value1.toString() + delim + value2.toString();
		}
	}
}
