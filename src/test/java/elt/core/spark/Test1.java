package elt.core.spark;

import java.util.ArrayList;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class Test1 {

	public static void main(String[] args) {
		ArrayList<Integer> integers = new ArrayList<>();
		int i = 10;
		integers.add(10);
		integers.add(20);

		System.out.println(i + ", " + integers);

		add(i, integers, 20);

		System.out.println(i + ", " + integers);
	}

	private static void add(int i, ArrayList<Integer> integers, int add) {
		i += add;
		for (int j = 0; j < integers.size(); j++) {
			integers.set(i, integers.get(i) + add);
		}
	}

}
