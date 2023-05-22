package com.anvizent.elt.core.spark.operation.cache;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeoutException;

import com.anvizent.elt.core.lib.exception.DateParseException;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidConfigValueException;
import com.anvizent.elt.core.lib.exception.InvalidSituationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedCoerceException;
import com.anvizent.elt.core.spark.constant.Constants.General;
import com.anvizent.elt.core.spark.exception.InvalidLookUpException;
import com.anvizent.elt.core.spark.operation.config.bean.RethinkRetrievalConfigBean;
import com.anvizent.elt.core.spark.operation.service.RethinkRetrievalFunctionService;
import com.anvizent.elt.core.spark.util.CollectionUtil;
import com.rethinkdb.gen.ast.Table;

import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
@SuppressWarnings("unchecked")
public class RethinkRetrievalCache extends Cache {

	public RethinkRetrievalCache(String name, RethinkRetrievalConfigBean rethinkRetrievalConfigBean) {
		super(name, rethinkRetrievalConfigBean.getMaxElementsInMemory(), null, true, null, false, Long.MAX_VALUE,
				rethinkRetrievalConfigBean.getTimeToIdleSeconds(), false, Long.MAX_VALUE, null);
	}

	public HashMap<String, Object> getCachedRow(HashMap<String, Object> row, Table table, RethinkRetrievalConfigBean rethinkRetrievalConfigBean)
			throws InvalidLookUpException, UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException,
			InvalidConfigValueException, UnimplementedException, SQLException, TimeoutException {
		HashMap<String, Object> newObject = new HashMap<>();
		HashMap<String, Object> whereKeyValues = CollectionUtil.getSubMap(row, rethinkRetrievalConfigBean.getWhereFields(),
				rethinkRetrievalConfigBean.getWhereColumns() == null || rethinkRetrievalConfigBean.getWhereColumns().isEmpty()
						? rethinkRetrievalConfigBean.getWhereFields() : rethinkRetrievalConfigBean.getWhereColumns());
		boolean cached = false;

		Element element = super.get(whereKeyValues);
		if (element == null) {
			HashMap<String, Object> lookUpRowObject = RethinkRetrievalFunctionService.getLookUpRow(row, table, rethinkRetrievalConfigBean);

			element = new Element(whereKeyValues, lookUpRowObject.get(General.LOOKUP_RESULTING_ROW));
			super.put(element);

			newObject.put(General.LOOKEDUP_ROWS_COUNT, lookUpRowObject.get(General.LOOKEDUP_ROWS_COUNT));
		} else {
			cached = true;
			newObject.put(General.LOOKEDUP_ROWS_COUNT, 0);
		}

		newObject.put(General.LOOKUP_CACHED, cached);
		newObject.put(General.LOOKUP_RESULTING_ROW, (LinkedHashMap<String, Object>) element.getObjectValue());

		return newObject;
	}

	public HashMap<String, Object> getCachedRows(HashMap<String, Object> row, Table table, RethinkRetrievalConfigBean rethinkRetrievalConfigBean)
			throws UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException, InvalidLookUpException,
			InvalidConfigValueException, UnimplementedException, SQLException, TimeoutException {
		HashMap<String, Object> newObject = new HashMap<>();
		HashMap<String, Object> whereKeyValues = CollectionUtil.getSubMap(row, rethinkRetrievalConfigBean.getWhereFields(),
				rethinkRetrievalConfigBean.getWhereColumns() == null || rethinkRetrievalConfigBean.getWhereColumns().isEmpty()
						? rethinkRetrievalConfigBean.getWhereFields() : rethinkRetrievalConfigBean.getWhereColumns());
		boolean cached = false;

		Element element = super.get(whereKeyValues);
		if (element == null) {
			HashMap<String, Object> fetcherRowObject = RethinkRetrievalFunctionService.getFetcherRows(row, table, rethinkRetrievalConfigBean);

			element = new Element(whereKeyValues, fetcherRowObject.get(General.LOOKUP_RESULTING_ROW));
			super.put(element);

			newObject.put(General.LOOKEDUP_ROWS_COUNT, fetcherRowObject.get(General.LOOKEDUP_ROWS_COUNT));
		} else {
			cached = true;
			newObject.put(General.LOOKEDUP_ROWS_COUNT, 0);
		}

		newObject.put(General.LOOKUP_CACHED, cached);
		newObject.put(General.LOOKUP_RESULTING_ROW, (ArrayList<LinkedHashMap<String, Object>>) element.getObjectValue());

		return newObject;
	}

}
