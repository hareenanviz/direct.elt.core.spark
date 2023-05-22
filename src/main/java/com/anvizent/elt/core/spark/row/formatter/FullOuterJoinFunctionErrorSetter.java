package com.anvizent.elt.core.spark.row.formatter;

import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.spark.api.java.Optional;

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
public class FullOuterJoinFunctionErrorSetter
		implements BaseAnvizentPairToNormalErrorSetter<Optional<HashMap<String, Object>>, Optional<HashMap<String, Object>>, HashMap<String, Object>> {

	private static final long serialVersionUID = 1L;

	public HashMap<String, Object> convertRow(JobDetails jobDetails, Tuple2<Optional<HashMap<String, Object>>, Optional<HashMap<String, Object>>> row,
			DataCorruptedException exception) {
		HashMap<String, Object> newRow;

		if (row == null) {
			newRow = new LinkedHashMap<>();
		} else if (row._1().isPresent()) {
			newRow = row._1().get();
		} else if (row._2().isPresent()) {
			newRow = row._2().get();
		} else {
			newRow = new LinkedHashMap<>();
		}

		LinkedHashMap<String, Object> errorRow = new LinkedHashMap<>();

		RowUtil.addJobDetails(jobDetails, errorRow);
		RowUtil.addEhDataPrefix(newRow, errorRow);
		RowUtil.addErrorDetails(exception, errorRow);
		RowUtil.addUserDetails(errorRow);

		return errorRow;
	}

}
