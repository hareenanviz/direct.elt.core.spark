package elt.core.spark.util;

import com.anvizent.elt.core.spark.util.IndexedAndLinkedMap;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class IndexedAndLinkedMapTest {

	public static void main(String[] args) {
		IndexedAndLinkedMap<String, String> map = new IndexedAndLinkedMap<>();

		map.put(0, "0", "ZERO");

		map.put(1, "1", "ONE");

		map.put(2, "2", "TWO");

		map.put(3, "3", "THREE");

		map.put(4, "4", "FOUR");

		map.put(5, "5", "FIVE");

		System.out.println("Main treeMap             :" + map);

		map.put(3, "6", "SIX");

		System.out.println("After modified 1 treeMap :" + map);

		map.put(6, "6", "SIX");

		System.out.println("After added new treeMap :" + map);
	}

}

