/**
 * 
 */
package com.anvizent.elt.core.spark.exception;

import com.anvizent.elt.core.spark.config.MapConfigurationReader;
import com.anvizent.elt.core.spark.util.LIFOQueue;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class DerivedComponentsLoopException extends Exception {
	private static final long serialVersionUID = 1L;

	public DerivedComponentsLoopException(LIFOQueue<MapConfigurationReader> mapConfigurationReaders, int index) {
		super(getMessage(mapConfigurationReaders, index));
	}

	private static String getMessage(LIFOQueue<MapConfigurationReader> mapConfigurationReaders, int index) {
		String loop = "";
		for (int i = index; i < mapConfigurationReaders.size(); i++) {
			if (!loop.isEmpty()) {
				loop += " -> ";
			}

			loop += mapConfigurationReaders.get(i).getName();
		}

		return "Referance loop \"" + loop + " -> " + mapConfigurationReaders.get(index).getName() + "\"";
	}
}
