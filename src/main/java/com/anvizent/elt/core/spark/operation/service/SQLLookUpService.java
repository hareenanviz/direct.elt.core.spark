package com.anvizent.elt.core.spark.operation.service;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeoutException;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.exception.DateParseException;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidConfigValueException;
import com.anvizent.elt.core.lib.exception.InvalidSituationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedCoerceException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.spark.constant.CacheType;
import com.anvizent.elt.core.spark.exception.InvalidLookUpException;
import com.anvizent.elt.core.spark.operation.config.bean.SQLLookUpConfigBean;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class SQLLookUpService extends SQLRetrievalService {

	private static final long serialVersionUID = 1L;

	public SQLLookUpService(SQLLookUpConfigBean sqlLookUpConfigBean, LinkedHashMap<String, AnvizentDataType> structure,
	        LinkedHashMap<String, AnvizentDataType> newStructure) throws ImproperValidationException, UnimplementedException, SQLException, TimeoutException {
		super(sqlLookUpConfigBean, structure, newStructure);
	}

	public HashMap<String, Object> selectFields(HashMap<String, Object> row)
	        throws InvalidLookUpException, SQLException, UnimplementedException, ImproperValidationException, TimeoutException {
		createConnection();

		if (sqlRetrievalConfigBean.getCacheType().equals(CacheType.NONE)) {
			return SQLRetrievalFunctionService.getLookUpRow((SQLLookUpConfigBean) sqlRetrievalConfigBean, selectPreparedStatement,
			        SQLRetrievalFunctionService.getWhereKeyValues(sqlRetrievalConfigBean, row));
		} else if (sqlRetrievalConfigBean.getCacheType().equals(CacheType.EHCACHE)) {
			return sqlRetrievalCache.getCachedRow((SQLLookUpConfigBean) sqlRetrievalConfigBean, bulkSelectPreparedStatement, selectPreparedStatement,
			        SQLRetrievalFunctionService.getWhereKeyValues(sqlRetrievalConfigBean, row));
		} else {
			throw new UnimplementedException("'" + sqlRetrievalConfigBean.getCacheType() + "' is not implemented.");
		}
	}

	public HashMap<String, Object> insertFields(HashMap<String, Object> row)
	        throws DateParseException, UnsupportedCoerceException, ImproperValidationException, InvalidSituationException, InvalidLookUpException,
	        ClassNotFoundException, ValidationViolationException, SQLException, InvalidConfigValueException, TimeoutException {
		createConnection();

		return SQLRetrievalFunctionService.checkInsertOnZeroFetch(sqlRetrievalConfigBean, insertPreparedStatement,
		        SQLRetrievalFunctionService.getWhereValues(sqlRetrievalConfigBean, row), row, structure, newStructure);
	}
}
