package com.anvizent.elt.core.spark.operation.cache;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.spark.TaskContext;

import com.anvizent.elt.core.spark.constant.Constants.ExceptionMessage;
import com.anvizent.elt.core.spark.constant.Constants.General;
import com.anvizent.elt.core.spark.exception.InvalidLookUpException;
import com.anvizent.elt.core.spark.operation.config.bean.SQLFetcherConfigBean;
import com.anvizent.elt.core.spark.operation.config.bean.SQLLookUpConfigBean;
import com.anvizent.elt.core.spark.operation.config.bean.SQLRetrievalConfigBean;
import com.anvizent.elt.core.spark.operation.service.SQLRetrievalFunctionService;

import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
@SuppressWarnings("unchecked")
public class SQLRetrievalCache extends Cache implements Serializable {

	private static final long serialVersionUID = 1L;

	public SQLRetrievalCache(String name, SQLRetrievalConfigBean sqlRetrievalConfigBean) {
		super(name, sqlRetrievalConfigBean.getMaxElementsInMemory(), MemoryStoreEvictionPolicy.LRU, true, null, false, Long.MAX_VALUE,
		        sqlRetrievalConfigBean.getTimeToIdleSeconds(), false, Long.MAX_VALUE, null);
	}

	public HashMap<String, Object> getCachedRow(SQLLookUpConfigBean sqlLookUpConfigBean, PreparedStatement bulkSelectPreparedStatement,
	        PreparedStatement selectPreparedStatement, LinkedHashMap<String, Object> whereKeyValues) throws InvalidLookUpException, SQLException {
		if (getSize() == 0) {
			synchronized (this) {
				if (getSize() == 0) {
					return bulkLookUpSelect(sqlLookUpConfigBean, bulkSelectPreparedStatement, whereKeyValues);
				} else {
					return singleSelect(sqlLookUpConfigBean, selectPreparedStatement, whereKeyValues);
				}
			}
		} else {
			return singleSelect(sqlLookUpConfigBean, selectPreparedStatement, whereKeyValues);
		}
	}

	private HashMap<String, Object> singleSelect(SQLLookUpConfigBean sqlLookUpConfigBean, PreparedStatement selectPreparedStatement,
	        LinkedHashMap<String, Object> whereKeyValues) throws InvalidLookUpException, SQLException {
		HashMap<String, Object> newObject = new HashMap<>();
		boolean cached = false;

		Element element = super.get(whereKeyValues);
		if (element == null) {
			HashMap<String, Object> resultSetRows = SQLRetrievalFunctionService.getLookUpRow(sqlLookUpConfigBean, selectPreparedStatement, whereKeyValues);

			element = new Element(whereKeyValues, resultSetRows.get(General.LOOKUP_RESULTING_ROW));
			super.put(element);

			newObject.put(General.LOOKEDUP_ROWS_COUNT, resultSetRows.get(General.LOOKEDUP_ROWS_COUNT));
		} else {
			cached = true;
			newObject.put(General.LOOKEDUP_ROWS_COUNT, 0);
		}

		newObject.put(General.LOOKUP_CACHED, cached);
		newObject.put(General.LOOKUP_RESULTING_ROW, element.getObjectValue());

		return newObject;
	}

	private HashMap<String, Object> bulkLookUpSelect(SQLLookUpConfigBean sqlLookUpConfigBean, PreparedStatement bulkSelectPreparedStatement,
	        HashMap<String, Object> whereKeyValues) throws SQLException, InvalidLookUpException {
		HashMap<String, Object> newObject = new HashMap<>();
		boolean cached = false;
		boolean throwError = false;

		Element element = super.get(whereKeyValues);
		if (element == null) {
			SQLRetrievalFunctionService.setBulkSelectPreparedStatement(bulkSelectPreparedStatement, whereKeyValues);
			System.out.println("execute query started ( " + new Date() + " ) => table => " + sqlLookUpConfigBean.getTableName()
			        + "(bulk select) => partition id => " + TaskContext.getPartitionId());
			ResultSet resultSet = bulkSelectPreparedStatement.executeQuery();
			System.out.println("execute query completed ( " + new Date() + " ) => table => " + sqlLookUpConfigBean.getTableName()
			        + "(bulk select) => partition id => " + TaskContext.getPartitionId());

			int rowsCount = 0;
			while (resultSet.next()) {
				HashMap<String, Object> lookUpRow = SQLRetrievalFunctionService.getRetrievalRowFromResultSet(resultSet, sqlLookUpConfigBean);
				HashMap<String, Object> cacheKeys = SQLRetrievalFunctionService.getCacheKeys(resultSet, sqlLookUpConfigBean);

				if (++rowsCount == 1 && !lookUpRow.values().containsAll(whereKeyValues.values())) {
					newObject.put(General.LOOKUP_RESULTING_ROW, null);
					newObject.put(General.LOOKEDUP_ROWS_COUNT, 0);
				} else if (rowsCount > 1 && sqlLookUpConfigBean.isLimitTo1() && lookUpRow.values().containsAll(whereKeyValues.values())) {
					throwError = true;
				} else if (super.get(cacheKeys) == null) {
					element = new Element(cacheKeys, lookUpRow);
					super.put(element);
					if (lookUpRow.values().containsAll(whereKeyValues.values())) {
						newObject.put(General.LOOKUP_RESULTING_ROW, lookUpRow);
						newObject.put(General.LOOKEDUP_ROWS_COUNT, 1);
					}
				} else {
					continue;
				}
			}

			if (throwError) {
				throw new InvalidLookUpException(
				        MessageFormat.format(ExceptionMessage.MORE_THAN_ONE_ROW_IN_LOOKUP_TABLE, sqlLookUpConfigBean.getTableName(), whereKeyValues));
			}
		} else {
			cached = true;
			newObject.put(General.LOOKEDUP_ROWS_COUNT, 0);
		}

		newObject.put(General.LOOKUP_CACHED, cached);

		return newObject;
	}

	public HashMap<String, Object> getCachedRows(SQLFetcherConfigBean sqlFetcherConfigBean, PreparedStatement selectPreparedStatement,
	        PreparedStatement bulkSelectPreparedStatement, LinkedHashMap<String, Object> whereKeyValues) throws InvalidLookUpException, SQLException {
		if (getSize() == 0) {
			synchronized (this) {
				if (getSize() == 0) {
					return bulkFetcherSelect(sqlFetcherConfigBean, bulkSelectPreparedStatement, whereKeyValues);
				} else {
					return fetcherSelect(sqlFetcherConfigBean, selectPreparedStatement, whereKeyValues);
				}
			}
		} else {
			return fetcherSelect(sqlFetcherConfigBean, selectPreparedStatement, whereKeyValues);
		}
	}

	private HashMap<String, Object> fetcherSelect(SQLFetcherConfigBean sqlFetcherConfigBean, PreparedStatement selectPreparedStatement,
	        LinkedHashMap<String, Object> whereKeyValues) throws InvalidLookUpException, SQLException {
		HashMap<String, Object> newObject = new HashMap<>();
		boolean cached = false;

		Element element = super.get(whereKeyValues);
		if (element == null) {
			HashMap<String, Object> resultSetRows = SQLRetrievalFunctionService.getFetcherRows(sqlFetcherConfigBean, selectPreparedStatement, whereKeyValues);

			element = new Element(whereKeyValues, resultSetRows.get(General.LOOKUP_RESULTING_ROW));
			super.put(element);

			newObject.put(General.LOOKEDUP_ROWS_COUNT, resultSetRows.get(General.LOOKEDUP_ROWS_COUNT));
		} else {
			cached = true;
			newObject.put(General.LOOKEDUP_ROWS_COUNT, 0);
		}

		newObject.put(General.LOOKUP_CACHED, cached);
		newObject.put(General.LOOKUP_RESULTING_ROW, element.getObjectValue());

		return newObject;
	}

	private HashMap<String, Object> bulkFetcherSelect(SQLFetcherConfigBean sqlFetcherConfigBean, PreparedStatement bulkSelectPreparedStatement,
	        HashMap<String, Object> whereKeyValues) throws SQLException, InvalidLookUpException {
		HashMap<String, Object> newObject = new HashMap<>();
		boolean cached = false;
		boolean throwError = false;

		Element element = super.get(whereKeyValues);
		if (element == null) {
			SQLRetrievalFunctionService.setBulkSelectPreparedStatement(bulkSelectPreparedStatement, whereKeyValues);
			System.out.println("execute query started ( " + new Date() + " ) => table => " + sqlFetcherConfigBean.getTableName() + "(bulk select)");
			ResultSet resultSet = bulkSelectPreparedStatement.executeQuery();
			System.out.println("execute query completed ( " + new Date() + " ) => table => " + sqlFetcherConfigBean.getTableName() + "(bulk select)");
			int rowsCount = 0;

			while (resultSet.next()) {
				ArrayList<HashMap<String, Object>> fetcherRows;
				HashMap<String, Object> fetcherRow = SQLRetrievalFunctionService.getRetrievalRowFromResultSet(resultSet, sqlFetcherConfigBean);
				HashMap<String, Object> cacheKeys = SQLRetrievalFunctionService.getCacheKeys(resultSet, sqlFetcherConfigBean);

				if (++rowsCount == 1 && !fetcherRow.values().containsAll(whereKeyValues.values())) {
					newObject.put(General.LOOKUP_RESULTING_ROW, null);
					newObject.put(General.LOOKEDUP_ROWS_COUNT, 0);
				}

				element = super.get(cacheKeys);
				if (element == null) {
					fetcherRows = new ArrayList<>();
					fetcherRows.add(fetcherRow);

					element = new Element(cacheKeys, fetcherRows);
					super.put(element);
				} else {
					fetcherRows = (ArrayList<HashMap<String, Object>>) element.getObjectValue();
					if (fetcherRows.size() < sqlFetcherConfigBean.getMaxFetchLimit()) {
						fetcherRows.add(fetcherRow);
						if (fetcherRow.values().containsAll(whereKeyValues.values())) {
							newObject.put(General.LOOKEDUP_ROWS_COUNT, fetcherRows.size());
							newObject.put(General.LOOKUP_RESULTING_ROW, fetcherRows);
						}
					} else if (fetcherRows.size() >= sqlFetcherConfigBean.getMaxFetchLimit() && fetcherRow.values().containsAll(whereKeyValues.values())) {
						throwError = true;
					} else {
						continue;
					}
				}
			}

			if (throwError) {
				throw new InvalidLookUpException(MessageFormat.format(ExceptionMessage.MORE_THAN_EXPECTED_ROWS_IN_FETCHER_TABLE,
				        sqlFetcherConfigBean.getMaxFetchLimit(), sqlFetcherConfigBean.getTableName(), whereKeyValues));
			}
		} else {
			cached = true;
			newObject.put(General.LOOKEDUP_ROWS_COUNT, 0);
		}

		newObject.put(General.LOOKUP_CACHED, cached);

		return newObject;
	}
}
