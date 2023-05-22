package com.anvizent.elt.core.spark.util;

import java.io.File;
import java.io.FilenameFilter;

import com.anvizent.elt.core.spark.constant.Constants;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class PartFilesFilter implements FilenameFilter {

	@Override
	public boolean accept(File dir, String name) {
		return name.startsWith(Constants.PART) && !name.endsWith(Constants.CRC);
	}
}
