package com.anvizent.elt.core.spark.operation.cache;

import java.io.IOException;
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
import com.anvizent.elt.core.spark.operation.config.bean.ArangoDBFetcherConfigBean;
import com.anvizent.elt.core.spark.operation.config.bean.ArangoDBLookUpConfigBean;
import com.anvizent.elt.core.spark.operation.config.bean.ArangoDBRetrievalConfigBean;
import com.anvizent.elt.core.spark.operation.service.ArangoDBRetrievalFunctionService;
import com.anvizent.query.builder.exception.UnderConstructionException;
import com.arangodb.ArangoDBException;

import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
@SuppressWarnings("unchecked")
public class ArangoDBRetrievalCache extends Cache {

	public ArangoDBRetrievalCache(String name, ArangoDBRetrievalConfigBean arangoDBRetrievalConfigBean) {
		super(name, arangoDBRetrievalConfigBean.getMaxElementsInMemory(), null, true, null, false, Long.MAX_VALUE,
				arangoDBRetrievalConfigBean.getTimeToIdleSeconds(), false, Long.MAX_VALUE, null);
	}

	public HashMap<String, Object> getCachedRow(ArangoDBLookUpConfigBean arangoDBRetrievalConfigBean, HashMap<String, Object> bindValues,
			HashMap<String, Object> row)
			throws ImproperValidationException, UnimplementedException, InvalidLookUpException, UnsupportedCoerceException, InvalidSituationException,
			DateParseException, SQLException, TimeoutException, IOException, InvalidConfigValueException, ArangoDBException, UnderConstructionException {
		HashMap<String, Object> newObject = new HashMap<>();

		boolean cached = false;

		Element element = super.get(bindValues);
		if (element == null) {
			HashMap<String, Object> lookUpRowObject = ArangoDBRetrievalFunctionService.getLookUpRow(arangoDBRetrievalConfigBean, row);

			element = new Element(bindValues, lookUpRowObject.get(General.LOOKUP_RESULTING_ROW));
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

	public HashMap<String, Object> getCachedRows(ArangoDBFetcherConfigBean arangoDBFetcherConfigBean, HashMap<String, Object> bindValues,
			HashMap<String, Object> row)
			throws InvalidLookUpException, UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException,
			UnimplementedException, InvalidConfigValueException, SQLException, TimeoutException, IOException, ArangoDBException, UnderConstructionException {
		HashMap<String, Object> newObject = new HashMap<>();

		boolean cached = false;

		Element element = super.get(bindValues);
		if (element == null) {
			HashMap<String, Object> fetcherRowObject = ArangoDBRetrievalFunctionService.getFetcherRows(arangoDBFetcherConfigBean, row);

			element = new Element(bindValues, fetcherRowObject.get(General.LOOKUP_RESULTING_ROW));
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
