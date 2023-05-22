package com.anvizent.elt.core.spark.operation.service;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.TimeoutException;

import com.anvizent.elt.core.lib.exception.DateParseException;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidConfigValueException;
import com.anvizent.elt.core.lib.exception.InvalidSituationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedCoerceException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.spark.constant.CacheType;
import com.anvizent.elt.core.spark.exception.InvalidLookUpException;
import com.anvizent.elt.core.spark.operation.config.bean.ArangoDBFetcherConfigBean;
import com.anvizent.query.builder.exception.UnderConstructionException;
import com.arangodb.ArangoDBException;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ArangoDBFetcherService extends ArangoDBRetrievalService {

	private static final long serialVersionUID = 1L;

	public ArangoDBFetcherService(ArangoDBFetcherConfigBean arangoDBFetcherConfigBean)
			throws UnimplementedException, ImproperValidationException, SQLException, TimeoutException {
		super(arangoDBFetcherConfigBean);
	}

	public HashMap<String, Object> selectFields(HashMap<String, Object> row)
			throws InvalidLookUpException, SQLException, UnimplementedException, ImproperValidationException, TimeoutException, UnsupportedCoerceException,
			InvalidSituationException, DateParseException, InvalidConfigValueException, IOException, ArangoDBException, UnderConstructionException {
		if (arangoDBRetrievalConfigBean.getCacheType().equals(CacheType.NONE)) {
			return ArangoDBRetrievalFunctionService.getFetcherRows((ArangoDBFetcherConfigBean) arangoDBRetrievalConfigBean, row);
		} else if (arangoDBRetrievalConfigBean.getCacheType().equals(CacheType.EHCACHE)) {
			return arangoDBRetrievalCache.getCachedRows((ArangoDBFetcherConfigBean) arangoDBRetrievalConfigBean,
					ArangoDBRetrievalFunctionService.getCacheBindValues((ArangoDBFetcherConfigBean) arangoDBRetrievalConfigBean, row), row);
		} else {
			throw new UnimplementedException("'" + arangoDBRetrievalConfigBean.getCacheType() + "' is not implemented.");
		}
	}

	public ArrayList<HashMap<String, Object>> insertFields()
			throws ImproperValidationException, SQLException, TimeoutException, DateParseException, UnsupportedCoerceException, InvalidSituationException,
			InvalidLookUpException, ClassNotFoundException, ValidationViolationException, InvalidConfigValueException {
		ArrayList<HashMap<String, Object>> newRows = new ArrayList<>();
		newRows.add(ArangoDBRetrievalFunctionService.checkInsertOnZeroFetch((ArangoDBFetcherConfigBean) arangoDBRetrievalConfigBean));

		return newRows;
	}
}
