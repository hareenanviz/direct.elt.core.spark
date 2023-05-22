package com.anvizent.elt.core.spark.operation.service;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.TimeoutException;

import com.anvizent.elt.core.lib.exception.DateParseException;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidConfigValueException;
import com.anvizent.elt.core.lib.exception.InvalidSituationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedCoerceException;
import com.anvizent.elt.core.spark.constant.CacheType;
import com.anvizent.elt.core.spark.exception.InvalidLookUpException;
import com.anvizent.elt.core.spark.operation.config.bean.RethinkRetrievalConfigBean;
import com.rethinkdb.gen.ast.Table;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class RethinkLookUpService extends RethinkRetrievalService {

	private static final long serialVersionUID = 1L;

	public RethinkLookUpService(RethinkRetrievalConfigBean rethinkRetrievalConfigBean)
			throws UnimplementedException, ImproperValidationException, TimeoutException, SQLException {
		super(rethinkRetrievalConfigBean);
	}

	public HashMap<String, Object> selectFields(HashMap<String, Object> row, Table table)
			throws InvalidLookUpException, UnimplementedException, UnsupportedCoerceException, InvalidSituationException, DateParseException,
			ImproperValidationException, InvalidConfigValueException, SQLException, TimeoutException {
		if (rethinkRetrievalConfigBean.getCacheType().equals(CacheType.NONE)) {
			return RethinkRetrievalFunctionService.getLookUpRow(row, table, rethinkRetrievalConfigBean);
		} else if (rethinkRetrievalConfigBean.getCacheType().equals(CacheType.EHCACHE)) {
			return rethinkRetrievalCache.getCachedRow(row, table, rethinkRetrievalConfigBean);
		} else {
			throw new UnimplementedException("'" + rethinkRetrievalConfigBean.getCacheType() + "' is not implemented.");
		}
	}

	public HashMap<String, Object> insertFields(HashMap<String, Object> row, Table table)
			throws InvalidLookUpException, UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException,
			InvalidConfigValueException, UnimplementedException, SQLException, TimeoutException {
		return RethinkRetrievalFunctionService.checkInsertOnZeroFetch(row, table, rethinkRetrievalConfigBean);
	}
}
