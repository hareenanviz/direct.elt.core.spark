package com.anvizent.elt.core.spark.sink.config.bean;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.anvizent.elt.core.spark.sink.util.bean.ArangoDBSinkGetResult;
import com.arangodb.ArangoDB;
import com.rethinkdb.RethinkDB;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class ArangoDBBatchDataAndConnections implements Serializable {

	private static final long serialVersionUID = 1L;

	private HashMap<Integer, ArangoDB> connections = new HashMap<>();
	private HashMap<Integer, ArrayList<ArangoDBSinkGetResult>> selectData = new HashMap<>();
	private HashMap<Integer, HashMap<String, HashMap<String, Object>>> unSelectedArangoDBKeys = new HashMap<>();
	private HashMap<Integer, ArrayList<HashMap<String, Object>>> keysAdded = new HashMap<>();
	private HashMap<Integer, List> insertData = new HashMap<>();
	private HashMap<Integer, List> updateData = new HashMap<>();

	public void addConnection(int partitionId, ArangoDB arangoDBConnection) {
		this.connections.put(partitionId, arangoDBConnection);
	}

	public ArangoDB getConnection(int paritionId) {
		return this.connections.get(paritionId);
	}

	public void addSelectData(int paritionId, ArangoDBSinkGetResult arangoDBSinkGetResult) {
		if (arangoDBSinkGetResult != null) {
			if (this.selectData.get(paritionId) == null) {
				this.selectData.put(paritionId, new ArrayList<>());
			}

			List records = this.selectData.get(paritionId);
			records.add(arangoDBSinkGetResult);
		}
	}

	public void clearSelectData(int paritionId) {
		this.selectData.remove(paritionId);
	}

	public ArrayList<ArangoDBSinkGetResult> getSelectData(int paritionId) {
		return this.selectData.get(paritionId);
	}

	public void addKeysAdded(int paritionId, HashMap<String, Object> keys) {
		if (keys != null && !keys.isEmpty()) {
			if (this.keysAdded.get(paritionId) == null) {
				this.keysAdded.put(paritionId, new ArrayList<>());
			}

			List records = this.keysAdded.get(paritionId);
			records.add(keys);
		}
	}

	public void clearKeysAdded(int paritionId) {
		this.keysAdded.remove(paritionId);
	}

	public ArrayList<HashMap<String, Object>> getKeysAdded(int paritionId) {
		return this.keysAdded.get(paritionId);
	}

	public void addInsertData(int paritionId, HashMap<String, Object> record) {
		if (record != null) {
			if (this.insertData.get(paritionId) == null) {
				this.insertData.put(paritionId, RethinkDB.r.array());
			}

			List rRecords = this.insertData.get(paritionId);
			rRecords.add(record);

			this.insertData.put(paritionId, rRecords);
		}
	}

	public void clearInsertData(int paritionId) {
		this.insertData.remove(paritionId);
	}

	public List getInsertData(int paritionId) {
		return this.insertData.get(paritionId);
	}

	public void addUnSelectedArangoDBKeys(int paritionId, String arangoDBKey, HashMap<String, Object> record) {
		if (arangoDBKey != null) {
			if (this.unSelectedArangoDBKeys.get(paritionId) == null) {
				this.unSelectedArangoDBKeys.put(paritionId, new HashMap<>());
			}

			HashMap<String, HashMap<String, Object>> unSelectedArangoDBKey = this.unSelectedArangoDBKeys.get(paritionId);
			unSelectedArangoDBKey.put(arangoDBKey, record);
		}
	}

	public void clearUnSelectedArangoDBKeys(int paritionId) {
		this.unSelectedArangoDBKeys.remove(paritionId);
	}

	public HashMap<String, HashMap<String, Object>> getUnSelectedArangoDBKeys(int paritionId) {
		return this.unSelectedArangoDBKeys.get(paritionId);
	}

	public void addUpdateData(int paritionId, HashMap<String, Object> record) {
		if (record != null) {
			if (this.updateData.get(paritionId) == null) {
				this.updateData.put(paritionId, RethinkDB.r.array());
			}

			List rRecords = this.updateData.get(paritionId);
			rRecords.add(record);

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
