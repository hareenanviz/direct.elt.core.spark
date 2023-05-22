package com.anvizent.elt.core.spark.sink.cache;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.listener.common.connection.RDBMSConnection;

import net.sf.ehcache.Cache;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.PersistenceConfiguration;
import net.sf.ehcache.config.PersistenceConfiguration.Strategy;

public abstract class SQLSinkCache extends Cache implements Serializable {
	private static final long serialVersionUID = 1L;

	protected RDBMSConnection rdbmsConnection;
	protected String table;
	protected ArrayList<String> keys;
	protected ArrayList<String> keyColumns;
	protected ArrayList<String> selectColumns;
	protected boolean keyFieldsCaseSensitive;

	public SQLSinkCache(String name, RDBMSConnection rdbmsConnection, String table, ArrayList<String> keys, ArrayList<String> keyColumns,
	        ArrayList<String> selectColumns, boolean keyFieldsCaseSensitive, Integer maxElementsInMemory) throws ImproperValidationException, SQLException {
		super(getCacheConfiguration(name, maxElementsInMemory));

		this.rdbmsConnection = rdbmsConnection;
		this.table = table;
		this.keys = keys;
		this.keyColumns = keyColumns;
		this.selectColumns = selectColumns;
		this.keyFieldsCaseSensitive = keyFieldsCaseSensitive;
	}

	private static CacheConfiguration getCacheConfiguration(String name, Integer maxElementsInMemory) {
		CacheConfiguration cacheConfiguration = new CacheConfiguration().name(name);

		if (maxElementsInMemory != null) {
			cacheConfiguration.maxEntriesLocalHeap(maxElementsInMemory);
		} else {
			cacheConfiguration.maxEntriesLocalHeap(0);
		}

		return cacheConfiguration.persistence(new PersistenceConfiguration().strategy(Strategy.LOCALTEMPSWAP)).maxEntriesLocalDisk(0).eternal(true);
	}

	public abstract RecordFromCache getSelectValues(HashMap<String, Object> row) throws ImproperValidationException, SQLException;

}
