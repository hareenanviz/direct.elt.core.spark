package com.anvizent.elt.core.spark.util;

import java.util.Collection;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class StringUtil {

	public static String nTimes(String string, int n) {
		if (n <= 0) {
			return "";
		} else {
			String result = string;
			while (true) {
				if ((result.length() / string.length()) * 2 < n) {
					result += result;
				} else if (result.length() / string.length() == n) {
					return result;
				} else {
					result += string;
				}
			}
		}
	}

	public static String[] join(String[] array1, String[] array2) {
		String[] array = new String[array1.length + array2.length];

		int i = 0;

		for (; i < array1.length; i++) {
			array[i] = array1[i];
		}

		for (int j = 0; j < array2.length; j++) {
			array[i++] = array2[j];
		}

		return array;
	}

	public static String[] join(String[] array1, String[] array2, String[] array3) {
		String[] array = new String[array1.length + array2.length + array3.length];
		System.out.println(array.length);
		int i = 0;

		for (; i < array1.length; i++) {
			array[i] = array1[i];
		}

		for (int j = 0; j < array2.length; j++) {
			array[i++] = array2[j];
		}

		for (int k = 0; k < array3.length; k++) {
			array[i++] = array3[k];
		}

		return array;
	}

	public static String replaceAt(String string, String replaceWith, int startIndex, int endIndex) {
		if (string == null || replaceWith == null) {
			throw new NullPointerException();
		}

		if (startIndex < 0 || startIndex >= string.length()) {
			throw new IndexOutOfBoundsException("Start Index is supposed to be in range: [0," + string.length() + ")");
		}

		if (endIndex < 0 || endIndex >= string.length()) {
			throw new IndexOutOfBoundsException("Start Index is supposed to be in range: [0," + string.length() + ")");
		}

		if (endIndex < startIndex) {
			throw new IndexOutOfBoundsException("Start index should be less than end index");
		}

		String firstPart;
		String lastPart;

		if (startIndex > 0) {
			firstPart = string.substring(0, startIndex);
		} else {
			firstPart = "";
		}

		if (endIndex < string.length()) {
			lastPart = string.substring(endIndex + 1);
		} else {
			lastPart = "";
		}

		return firstPart + replaceWith + lastPart;
	}

	public static String[] splitByFirstOccurance(String string, String... splitBy) {
		if (string == null) {
			throw new NullPointerException();
		}

		int minIndex = -1;
		int minIndexLength = -1;

		for (int i = 0; i < splitBy.length; i++) {
			if (splitBy[i] == null) {
				throw new NullPointerException();
			} else if (splitBy[i].length() == 0) {
				return new String[] { string.substring(0, 1), string.substring(1) };
			} else {
				int index = string.indexOf(splitBy[i]);
				if (index != -1 && (minIndex == -1 || index < minIndex)) {
					minIndex = index;
					minIndexLength = splitBy[i].length();
				}
			}
		}

		if (minIndex == -1) {
			return new String[] { string };
		} else {
			return new String[] { string.substring(0, minIndex), string.substring(minIndex + minIndexLength) };
		}
	}

	public static String join(Collection<String> collection, String joinBy, char metaChar, char escapeChar) {
		String join = "";
		for (String keyColumn : collection) {
			if (!join.isEmpty()) {
				join += joinBy;
			}

			join += metaChar + addMetaChar(keyColumn, metaChar, escapeChar) + metaChar;
		}

		return join;
	}

	public static int indexBefore(String string, int index, char c) {
		while (index-- > -1) {
			if (string.charAt(index) == c) {
				return index;
			}
		}

		return -1;
	}

	public static String getQueryHead(String tableName, String query) {
		int rank = rankIn('(', tableName);
		int index = 0;
		while (rank-- >= 0 && (index = query.indexOf('(', index)) != -1)
			;

		return query.substring(0, index + 1);
	}

	public static String removeMetaChar(String string, char metaChar, char escapeChar) {
		string = string.replace("" + escapeChar + escapeChar, "" + escapeChar);
		return string.replace("" + escapeChar + metaChar, "" + metaChar);
	}

	public static String addMetaChar(String string, char metaChar, char escapeChar) {
		string = string.replace("" + escapeChar, "" + escapeChar + escapeChar);
		return string.replace("" + metaChar, "" + escapeChar + metaChar);
	}

	public static int rankIn(char c, String tableName) {
		int index = 0;
		int rank = 0;

		while ((index = tableName.indexOf(c, index)) != -1) {
			rank++;
		}

		return rank;
	}
}
