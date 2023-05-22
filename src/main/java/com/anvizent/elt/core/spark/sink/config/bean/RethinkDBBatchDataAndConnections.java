package com.anvizent.elt.core.spark.sink.config.bean;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.anvizent.elt.core.spark.sink.util.bean.RethinkDBSinkGetResult;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.net.Connection;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class RethinkDBBatchDataAndConnections implements Serializable {

	private static final long serialVersionUID = 1L;

	private HashMap<Integer, Connection> connections = new HashMap<>();
	private HashMap<Integer, ArrayList<RethinkDBSinkGetResult>> selectData = new HashMap<>();
	private HashMap<Integer, HashMap<Object, HashMap<String, Object>>> unSelectedRIds = new HashMap<>();
	private HashMap<Integer, ArrayList<HashMap<String, Object>>> keysAdded = new HashMap<>();
	private HashMap<Integer, List> insertData = new HashMap<>();
	private HashMap<Integer, List> updateData = new HashMap<>();

	public void addConnection(int partitionId, Connection connection) {
		this.connections.put(partitionId, connection);
	}

	public Connection getConnection(int paritionId) {
		return this.connections.get(paritionId);
	}

	public void addSelectData(int paritionId, RethinkDBSinkGetResult rethinkDBSinkGetResult) {
		if (rethinkDBSinkGetResult != null) {
			if (this.selectData.get(paritionId) == null) {
				this.selectData.put(paritionId, new ArrayList<>());
			}

			List rRecords = this.selectData.get(paritionId);
			rRecords.add(rethinkDBSinkGetResult);
		}
	}

	public void clearSelectData(int paritionId) {
		this.selectData.remove(paritionId);
	}

	public ArrayList<RethinkDBSinkGetResult> getSelectData(int paritionId) {
		return this.selectData.get(paritionId);
	}

	public void addKeysAdded(int paritionId, HashMap<String, Object> keys) {
		if (keys != null && !keys.isEmpty()) {
			if (this.keysAdded.get(paritionId) == null) {
				this.keysAdded.put(paritionId, new ArrayList<>());
			}

			List rRecords = this.keysAdded.get(paritionId);
			rRecords.add(keys);
		}
	}

	public void clearKeysAdded(int paritionId) {
		this.keysAdded.remove(paritionId);
	}

	public ArrayList<HashMap<String, Object>> getKeysAdded(int paritionId) {
		return this.keysAdded.get(paritionId);
	}

	public void addInsertData(int paritionId, MapObject rRecord) {
		if (rRecord != null) {
			if (this.insertData.get(paritionId) == null) {
				this.insertData.put(paritionId, RethinkDB.r.array());
			}

			List rRecords = this.insertData.get(paritionId);
			rRecords.add(rRecord);

			this.insertData.put(paritionId, rRecords);
		}
	}

	public void clearInsertData(int paritionId) {
		this.insertData.remove(paritionId);
	}

	public List getInsertData(int paritionId) {
		return this.insertData.get(paritionId);
	}

	public void addUnSelectedRIds(int paritionId, Object rId, HashMap<String, Object> row) {
		if (rId != null) {
			if (this.unSelectedRIds.get(paritionId) == null) {
				this.unSelectedRIds.put(paritionId, new HashMap<>());
			}

			HashMap<Object, HashMap<String, Object>> unSelectedRId = this.unSelectedRIds.get(paritionId);
			unSelectedRId.put(rId, row);
		}
	}

	public void clearUnSelectedRIds(int paritionId) {
		this.unSelectedRIds.remove(paritionId);
	}

	public HashMap<Object, HashMap<String, Object>> getUnSelectedRIds(int paritionId) {
		return this.unSelectedRIds.get(paritionId);
	}

	public void addUpdatetData(int paritionId, MapObject rRecord) {
		if (rRecord != null) {
			if (this.updateData.get(paritionId) == null) {
				this.updateData.put(paritionId, RethinkDB.r.array());
			}

			List rRecords = this.updateData.get(paritionId);
			rRecords.add(rRecord);

			this.updateData.put(paritionId, rRecords);
		}
	}

	public void clearUpdateData(int paritionId) {
		this.updateData.remove(paritionId);
	}

	public List getUpdateData(int paritionId) {
		return this.updateData.get(paritionId);
	}
}
