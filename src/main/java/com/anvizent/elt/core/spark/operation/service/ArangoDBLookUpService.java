package com.anvizent.elt.core.spark.operation.service;

import java.io.IOException;
import java.sql.SQLException;
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
import com.anvizent.elt.core.spark.operation.config.bean.ArangoDBLookUpConfigBean;
import com.anvizent.query.builder.exception.UnderConstructionException;
import com.arangodb.ArangoDBException;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ArangoDBLookUpService extends ArangoDBRetrievalService {

	private static final long serialVersionUID = 1L;

	public ArangoDBLookUpService(ArangoDBLookUpConfigBean arangoDBLookUpConfigBean)
			throws ImproperValidationException, UnimplementedException, SQLException, TimeoutException {
		super(arangoDBLookUpConfigBean);
	}

	public HashMap<String, Object> selectFields(HashMap<String, Object> row) throws UnimplementedException, ImproperValidationException, TimeoutException,
			SQLException, InvalidLookUpException, IOException, UnsupportedCoerceException, InvalidSituationException, DateParseException,
			InvalidConfigValueException, ArangoDBException, UnderConstructionException {
		if (arangoDBRetrievalConfigBean.getCacheType().equals(CacheType.NONE)) {
			return ArangoDBRetrievalFunctionService.getLookUpRow((ArangoDBLookUpConfigBean) arangoDBRetrievalConfigBean, row);
		} else if (arangoDBRetrievalConfigBean.getCacheType().equals(CacheType.EHCACHE)) {
			return arangoDBRetrievalCache.getCachedRow((ArangoDBLookUpConfigBean) arangoDBRetrievalConfigBean,
					ArangoDBRetrievalFunctionService.getCacheBindValues(arangoDBRetrievalConfigBean, row), row);
		} else {
			throw new UnimplementedException("'" + arangoDBRetrievalConfigBean.getCacheType() + "' is not implemented.");
		}
	}

	public HashMap<String, Object> insertFields() throws DateParseException, UnsupportedCoerceException, ImproperValidationException, InvalidSituationException,
			InvalidLookUpException, ClassNotFoundException, ValidationViolationException, SQLException, InvalidConfigValueException, TimeoutException {

		return ArangoDBRetrievalFunctionService.checkInsertOnZeroFetch((ArangoDBLookUpConfigBean) arangoDBRetrievalConfigBean);
	}
}
