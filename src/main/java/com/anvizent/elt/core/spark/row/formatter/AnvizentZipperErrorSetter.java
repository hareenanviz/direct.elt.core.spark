package com.anvizent.elt.core.spark.row.formatter;

import java.util.HashMap;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.exception.DataCorruptedException;
import com.anvizent.elt.core.lib.row.formatter.BaseAnvizentPairToNormalErrorSetter;
import com.anvizent.elt.core.lib.util.RowUtil;

import scala.Tuple2;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class AnvizentZipperErrorSetter implements BaseAnvizentPairToNormalErrorSetter<HashMap<String, Object>, Long, HashMap<String, Object>> {

	private static final long serialVersionUID = 1L;

	public LinkedHashMap<String, Object> convertRow(JobDetails jobDetails, Tuple2<HashMap<String, Object>, Long> row, DataCorruptedException exception) {
		LinkedHashMap<String, Object> errorRow = new LinkedHashMap<>();

		RowUtil.addJobDetails(jobDetails, errorRow);
		RowUtil.addEhDataPrefix(row._1(), errorRow);
		RowUtil.addErrorDetails(exception, errorRow);
		RowUtil.addUserDetails(errorRow);

		return errorRow;
	}

}
