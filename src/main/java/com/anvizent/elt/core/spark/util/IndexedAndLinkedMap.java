package com.anvizent.elt.core.spark.util;

import java.io.Serializable;
import java.util.ArrayList;

import com.anvizent.elt.core.spark.constant.Constants.General;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class IndexedAndLinkedMap<K, V> implements Serializable {
	private static final long serialVersionUID = 1L;

	private ArrayList<K> keys = new ArrayList<>();
	private ArrayList<V> values = new ArrayList<>();

	public int size() {
		return keys.size();
	}

	public boolean isEmpty() {
		return keys.isEmpty();
	}

	public boolean containsKey(K key) {
		return keys.contains(key);
	}

	public boolean containsValue(V value) {
		return values.contains(value);
	}

	public V get(K key) {
		return values.get(keys.indexOf(key));
	}

	public V put(Integer index, K key, V value) {
		if (index < 0 || index > keys.size()) {
			throw new ArrayIndexOutOfBoundsException(index);
		} else {
			int keyIndex = keys.indexOf(key);
			if (keyIndex == -1) {
				keys.add(index, key);
				values.add(index, value);
			} else {
				keys.remove(keyIndex);
				V previousValue = values.remove(keyIndex);

				keys.add(index, key);
				values.add(index, value);

				return previousValue;
			}
		}

		return null;
	}

	public V putFirst(K key, V value) {
		return put(0, key, value);
	}

	public V putLast(K key, V value) {
		return put(keys.size(), key, value);
	}

	public V remove(K key) {
		int index = keys.indexOf(key);

		if (index == -1) {
			return null;
		} else {
			keys.remove(index);
			return values.remove(index);
		}
	}

	public void clear() {
		keys.clear();
		values.clear();
	}

	public ArrayList<K> keys() {
		return keys;
	}

	public ArrayList<V> values() {
		return values;
	}

	@Override
	public String toString() {
		StringBuilder stringBuilder = new StringBuilder();
		for (int i = 0; i < keys.size(); i++) {
			if (stringBuilder.length() != 0) {
				stringBuilder.append(General.CONFIG_SEPARATOR + " ");
			}

			stringBuilder.append(keys.get(i) + " " + General.Operator.EQUAL_TO + " " + values.get(i));
		}

		return General.OPEN_BRACE + " " + stringBuilder + " " + General.CLOSE_BRACE;
	}
}