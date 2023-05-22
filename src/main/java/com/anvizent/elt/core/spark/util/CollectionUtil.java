package com.anvizent.elt.core.spark.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class CollectionUtil implements Serializable {
	private static final long serialVersionUID = 1L;

	public static <T> T getFromList(ArrayList<T> list, int index) {
		return getFromList(list, index, null);
	}

	public static <T> T getFromList(ArrayList<T> list, int index, T defaultValue) {
		if (list == null) {
			return defaultValue;
		} else {
			T t = list.get(index);
			if (t == null) {
				return defaultValue;
			} else {
				return t;
			}
		}
	}

	public static <T> boolean hasAnyCommonElements(Collection<T> collection1, Collection<T> collection2) {
		if (collection1 == null || collection2 == null) {
			return false;
		}

		for (T element : collection1) {
			if (collection2.contains(element)) {
				return true;
			}
		}

		return false;
	}

	public static <T> int getConflictingValuesIndex(List<T> collection1, List<T> collection2) {
		if (collection1 == null || collection2 == null || collection1.size() != collection2.size()) {
			return -1;
		}

		boolean string = false;

		for (int i = 0; i < collection1.size(); i++) {
			if (string) {
				if (collection1.get(i) != null && !((String) collection1.get(i)).isEmpty() && collection2.get(i) != null
				        && !((String) collection2.get(i)).isEmpty()) {
					return i;
				}
			} else {
				if (collection1.get(i) != null && collection2.get(i) != null) {
					if (collection1.get(i).getClass().equals(String.class)) {
						string = true;
						if (!((String) collection1.get(i)).isEmpty() && !((String) collection2.get(i)).isEmpty()) {
							return i;
						}
					} else {
						return i;
					}
				}
			}
		}

		return -1;
	}

	public static <K, T> LinkedHashMap<K, T> removeAll(LinkedHashMap<K, T> map, Collection<K> keysToRemove) {
		if (map == null || keysToRemove == null) {
			return null;
		}

		LinkedHashMap<K, T> removed = new LinkedHashMap<>();

		for (K key : keysToRemove) {
			removed.put(key, map.remove(key));
		}

		return removed;
	}

	public static <V> void putWithPrefix(LinkedHashMap<String, V> map, String prefix, String key, V value) {
		if (map == null || prefix == null || key == null) {
			return;
		}

		String newKey = prefix + key;

		if (map.containsKey(newKey)) {
			LinkedHashMap<String, V> copy = new LinkedHashMap<>(map);
			map.clear();

			for (Entry<String, V> entry : copy.entrySet()) {
				if (entry.getKey().equals(newKey)) {
					putWithPrefix(map, prefix, newKey, entry.getValue());
				} else {
					map.put(entry.getKey(), entry.getValue());
				}
			}
		}

		map.put(newKey, value);
	}

	public static <K, V> HashMap<K, V> getSubMap(HashMap<K, V> map, Collection<K> fields, Collection<K> columns) {
		if (map == null || fields == null) {
			return null;
		}

		HashMap<K, V> subMap = new HashMap<>();

		Iterator<K> iterator = columns.iterator();
		for (K field : fields) {
			subMap.put(iterator.next(), map.get(field));
		}

		return subMap;
	}

	public static <K, V> HashMap<K, V> generateMap(Collection<K> fields, Collection<V> values) {
		if (fields == null || values == null) {
			return null;
		}

		HashMap<K, V> newMap = new HashMap<>();

		Iterator<V> iterator = values.iterator();
		for (K field : fields) {
			newMap.put(field, iterator.next());
		}

		return newMap;
	}

	public static <K, V> void putConstants(Map<K, V> map, Collection<K> fields, Object value) {
		if (map != null && fields != null && !fields.isEmpty()) {
			for (K field : fields) {
				map.put(field, null);
			}
		}
	}

	public static boolean isEmpty(Collection<?> data) {
		return data == null || data.isEmpty();
	}

	public static boolean isIn(String fieldToSearch, ArrayList<String> fields, boolean caseSensitive) {
		if (CollectionUtils.isEmpty(fields)) {
			return false;
		} else if (caseSensitive || fieldToSearch == null) {
			return fields.contains(fieldToSearch);
		} else {
			for (String field : fields) {
				if (fieldToSearch.equalsIgnoreCase(field)) {
					return true;
				}
			}

			return false;
		}
	}

	public static int indexOf(String fieldToSearch, ArrayList<String> fields, boolean caseSensitive) {
		if (CollectionUtils.isEmpty(fields)) {
			return -1;
		} else if (caseSensitive || fieldToSearch == null) {
			return fields.indexOf(fieldToSearch);
		} else {
			for (int i = 0; i < fields.size(); i++) {
				if (fieldToSearch.equalsIgnoreCase(fields.get(i))) {
					return i;
				}
			}

			return -1;
		}
	}

	public static boolean isAnyEmpty(Collection<String> data) {
		if (data == null || data.isEmpty()) {
			return true;
		}

		boolean isAnyEmpty = false;

		for (String e : data) {
			if (StringUtils.isEmpty(e)) {
				isAnyEmpty = true;
				break;
			}
		}

		return isAnyEmpty;
	}

	public static boolean isAllEmpty(Collection<String> data) {
		if (data == null || data.isEmpty()) {
			return true;
		}

		boolean isAllEmpty = true;

		for (String e : data) {
			if (StringUtils.isNotEmpty(e)) {
				isAllEmpty = false;
				break;
			}
		}

		return isAllEmpty;
	}

	public static boolean isAnyNotEmpty(Collection<String> data) {
		if (data == null || data.isEmpty()) {
			return false;
		}

		boolean isAnyNotEmpty = false;

		for (String e : data) {
			if (StringUtils.isNotEmpty(e)) {
				isAnyNotEmpty = true;
				break;
			}
		}

		return isAnyNotEmpty;
	}

	public static boolean isAllNotEmpty(Collection<String> data) {
		if (data == null || data.isEmpty()) {
			return false;
		}

		boolean isAllNotEmpty = true;

		for (String e : data) {
			if (StringUtils.isEmpty(e)) {
				isAllNotEmpty = false;
				break;
			}
		}

		return isAllNotEmpty;
	}

	public static <T> boolean isAnyNull(Collection<T> data) {
		if (data == null || data.isEmpty()) {
			return true;
		}

		boolean isAnyNull = false;

		for (T e : data) {
			if (e == null) {
				isAnyNull = true;
				break;
			}
		}

		return isAnyNull;
	}

	public static <T> boolean isAllNull(Collection<T> data) {
		if (data == null || data.isEmpty()) {
			return true;
		}

		boolean isAllNull = true;

		for (T e : data) {
			if (e != null) {
				isAllNull = false;
				break;
			}
		}

		return isAllNull;
	}

	public static <T> boolean isAllNotNull(Collection<T> data) {
		if (data == null || data.isEmpty()) {
			return false;
		}

		boolean isAllNotEmpty = true;

		for (T e : data) {
			if (e == null) {
				isAllNotEmpty = false;
				break;
			}
		}

		return isAllNotEmpty;
	}
}