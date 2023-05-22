package com.anvizent.elt.core.spark.sink.service;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import com.anvizent.elt.core.lib.exception.UnsupportedCoerceException;
import com.anvizent.elt.core.lib.util.TypeConversionUtil;
import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.gen.ast.ReqlFunction1;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ReqlFilterFunction implements ReqlFunction1 {

	private HashMap<String, Object> row;
	private ArrayList<String> eqFileds;
	private ArrayList<String> eqColumns;
	private ArrayList<String> ltFileds;
	private ArrayList<String> ltColumns;
	private ArrayList<String> gtFileds;
	private ArrayList<String> gtColumns;
	private ZoneOffset timeZoneOffset;

	public ReqlFilterFunction(HashMap<String, Object> row, ArrayList<String> eqFileds, ArrayList<String> eqColumns, ZoneOffset timeZoneOffset) {
		this.row = row;
		this.eqFileds = eqFileds;
		this.eqColumns = eqColumns;
		this.timeZoneOffset = timeZoneOffset;
	}

	public ReqlFilterFunction(HashMap<String, Object> row, ArrayList<String> eqFileds, ArrayList<String> eqColumns, ArrayList<String> ltFileds,
			ArrayList<String> ltColumns, ArrayList<String> gtFileds, ArrayList<String> gtColumns, ZoneOffset timeZoneOffset) {
		this.row = row;
		this.eqFileds = eqFileds;
		this.eqColumns = eqColumns;
		this.ltFileds = ltFileds;
		this.ltColumns = ltColumns;
		this.gtFileds = gtFileds;
		this.gtColumns = gtColumns;
		this.timeZoneOffset = timeZoneOffset;
	}

	@Override
	public Object apply(ReqlExpr initRow) {
		ReqlExpr reqlExpr;

		try {
			reqlExpr = applyEqExpr(initRow);
			reqlExpr = applyLtExpr(initRow, reqlExpr);
			reqlExpr = applyGtExpr(initRow, reqlExpr);

			if (reqlExpr == null) {
				return initRow;
			} else {
				return reqlExpr;
			}
		} catch (UnsupportedCoerceException exception) {
			throw new RuntimeException(exception.getMessage(), exception);
		}
	}

	private ReqlExpr applyEqExpr(ReqlExpr initRow) throws UnsupportedCoerceException {
		ReqlExpr reqlExpr;

		if (eqFileds == null || eqFileds.size() == 0) {
			return null;
		}

		if (eqColumns != null && eqColumns.size() > 0) {
			reqlExpr = initRow.g(eqColumns.get(0)).eq(getRowValue(eqFileds.get(0)));
		} else {
			reqlExpr = initRow.g(eqFileds.get(0)).eq(getRowValue(eqFileds.get(0)));
		}

		for (int i = 1; i < eqFileds.size(); i++) {
			if (eqColumns != null && eqColumns.size() > 0) {
				reqlExpr = reqlExpr.and(initRow.g(eqColumns.get(i)).eq(getRowValue(eqFileds.get(i))));
			} else {
				reqlExpr = reqlExpr.and(initRow.g(eqFileds.get(i)).eq(getRowValue(eqFileds.get(i))));
			}
		}

		return reqlExpr;
	}

	private ReqlExpr applyLtExpr(ReqlExpr initRow, ReqlExpr reqlExpr) throws UnsupportedCoerceException {
		if (ltFileds == null || ltFileds.size() == 0) {
			return reqlExpr;
		}

		int i = 0;

		if (reqlExpr == null) {
			if (ltColumns != null && ltColumns.size() > 0) {
				reqlExpr = initRow.g(ltColumns.get(0)).lt(getRowValue(ltFileds.get(0)));
			} else {
				reqlExpr = initRow.g(ltFileds.get(0)).lt(getRowValue(ltFileds.get(0)));
			}

			i++;
		}

		for (; i < ltFileds.size(); i++) {
			if (ltColumns != null && ltColumns.size() > 0) {
				reqlExpr = reqlExpr.and(initRow.g(ltColumns.get(i)).lt(getRowValue(ltFileds.get(i))));
			} else {
				reqlExpr = reqlExpr.and(initRow.g(ltFileds.get(i)).lt(getRowValue(ltFileds.get(i))));
			}
		}

		return reqlExpr;
	}

	private ReqlExpr applyGtExpr(ReqlExpr initRow, ReqlExpr reqlExpr) throws UnsupportedCoerceException {
		if (gtFileds == null || gtFileds.size() == 0) {
			return reqlExpr;
		}

		int i = 0;

		if (reqlExpr == null) {
			if (gtColumns != null && gtColumns.size() > 0) {
				reqlExpr = initRow.g(gtColumns.get(0)).gt(getRowValue(gtFileds.get(0)));
			} else {
				reqlExpr = initRow.g(gtFileds.get(0)).gt(getRowValue(gtFileds.get(0)));
			}

			i++;
		}

		for (; i < gtFileds.size(); i++) {
			if (gtColumns != null && gtColumns.size() > 0) {
				reqlExpr = reqlExpr.and(initRow.g(gtColumns.get(i)).gt(getRowValue(gtFileds.get(i))));
			} else {
				reqlExpr = reqlExpr.and(initRow.g(gtFileds.get(i)).gt(getRowValue(gtFileds.get(i))));
			}
		}

		return reqlExpr;
	}

	private Object getRowValue(String key) throws UnsupportedCoerceException {
		Object rowValue = row.get(key);

		if (rowValue != null && rowValue.getClass().equals(Date.class)) {
			OffsetDateTime offsetDateTime =
					(OffsetDateTime) TypeConversionUtil.dateToOffsetDateTypeConversion(rowValue, Date.class, OffsetDateTime.class, timeZoneOffset);
			return offsetDateTime;
		}

		return rowValue;
	}

}
