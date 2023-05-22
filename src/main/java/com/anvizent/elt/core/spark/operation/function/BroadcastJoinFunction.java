package com.anvizent.elt.core.spark.operation.function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.spark.broadcast.Broadcast;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.exception.DataCorruptedException;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.lib.function.AnvizentFlatMapFunction;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.spark.constant.JoinType;
import com.anvizent.elt.core.spark.operation.config.bean.JoinConfigBean;

/**
 * @author Hareen Bejjanki
 *
 */
public class BroadcastJoinFunction extends AnvizentFlatMapFunction {

	private static final long serialVersionUID = 1L;

	protected final List<String> keys;
	protected final HashMap<String, String> newNames;
	protected final HashMap<String, String> braodcastNewNames;
	@SuppressWarnings("rawtypes")
	private final Broadcast<HashMap> broadcastData;
	private final JoinType joinType;

	@SuppressWarnings("rawtypes")
	public BroadcastJoinFunction(JoinConfigBean joinConfigBean, LinkedHashMap<String, AnvizentDataType> structure,
	        LinkedHashMap<String, AnvizentDataType> newStructure, List<String> keys, HashMap<String, String> newNames,
	        HashMap<String, String> braodcastNewNames, JoinType joinType, Broadcast<HashMap> broadcastData, ArrayList<AnvizentAccumulator> accumulators,
	        AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails) throws InvalidArgumentsException {
		super(joinConfigBean, null, structure, newStructure, null, accumulators, errorHandlerSinkFunction, jobDetails);
		this.keys = keys;
		this.newNames = newNames;
		this.braodcastNewNames = braodcastNewNames;
		this.broadcastData = broadcastData;
		this.joinType = joinType;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Iterator<HashMap<String, Object>> process(HashMap<String, Object> row)
	        throws RecordProcessingException, ValidationViolationException, DataCorruptedException {
		LinkedList<Object> key = getRowKey(row);
		HashMap<String, Object> braodcastRow = (HashMap<String, Object>) broadcastData.getValue().get(key);
		ArrayList<HashMap<String, Object>> list = new ArrayList<>(1);

		if (braodcastRow != null) {
			HashMap<String, Object> newRow = getNewRow(row, braodcastRow);
			list.add(newRow);
		} else if (!joinType.equals(JoinType.SIMPLE_JOIN)) {
			HashMap<String, Object> newRow = getNewRow(row, null);
			list.add(newRow);
		}

		return list.iterator();
	}

	private HashMap<String, Object> getNewRow(HashMap<String, Object> row, HashMap<String, Object> braodcastRow) {
		HashMap<String, Object> newRow = new HashMap<>();

		putAll(newRow, row, newNames);
		if (braodcastRow != null) {
			putAll(newRow, braodcastRow, braodcastNewNames);
		}

		return newRow;
	}

	private void putAll(HashMap<String, Object> newRow, HashMap<String, Object> row, HashMap<String, String> newNames) {
		for (Entry<String, Object> data : row.entrySet()) {
			String key = data.getKey();

			if (newNames.containsKey(key)) {
				key = newNames.get(key);
			}

			newRow.put(key, data.getValue());
		}
	}

	private LinkedList<Object> getRowKey(HashMap<String, Object> row) {
		LinkedList<Object> rowKey = new LinkedList<>();

		for (String joinKey : keys) {
			if (newNames.containsKey(joinKey)) {
				joinKey = newNames.get(joinKey);
			}

			rowKey.add(row.get(joinKey));
		}

		return rowKey;
	}
}
