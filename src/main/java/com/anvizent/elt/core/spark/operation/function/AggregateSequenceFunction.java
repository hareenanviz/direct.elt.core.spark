package com.anvizent.elt.core.spark.operation.function;

import java.util.HashMap;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.exception.DataCorruptedException;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.lib.function.AnvizentFunction2;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.spark.constant.Aggregations;
import com.anvizent.elt.core.spark.exception.AggregationException;
import com.anvizent.elt.core.spark.operation.config.bean.GroupByConfigBean;
import com.anvizent.elt.core.spark.operation.service.AggregationService;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class AggregateSequenceFunction extends AnvizentFunction2 {

	private static final long serialVersionUID = 1L;

	private HashMap<String, String[]> averageFields;

	public AggregateSequenceFunction(ConfigBean configBean, LinkedHashMap<String, AnvizentDataType> structure,
	        LinkedHashMap<String, AnvizentDataType> newStructure, HashMap<String, String[]> averageFields, AnvizentVoidFunction errorHandlerSinkFunction,
	        JobDetails jobDetails) throws InvalidArgumentsException {
		super(configBean, null, structure, newStructure, null, null, errorHandlerSinkFunction, jobDetails);
		this.averageFields = averageFields;
	}

	@Override
	public HashMap<String, Object> process(HashMap<String, Object> row_1, HashMap<String, Object> row_2)
	        throws ValidationViolationException, RecordProcessingException, DataCorruptedException {
		LinkedHashMap<String, Object> newRow = new LinkedHashMap<String, Object>();

		putGroupByRow(newRow, row_2);
		putAggregations(newRow, row_1, row_2);

		return newRow;
	}

	private void putAggregations(HashMap<String, Object> newRow, HashMap<String, Object> row_1, HashMap<String, Object> row_2) throws DataCorruptedException {
		try {
			GroupByConfigBean groupByConfigBean = (GroupByConfigBean) configBean;

			for (int i = 0; i < groupByConfigBean.getAggregationFields().size(); i++) {
				String aggregationField = groupByConfigBean.getAggregationFields().get(i);
				Aggregations aggregationType = groupByConfigBean.getAggregations().get(i);
				String aggregationFieldsAlias = groupByConfigBean.getAliasNames().get(i);

				if (aggregationType.equals(Aggregations.SUM)) {
					newRow.put(aggregationFieldsAlias, AggregationService.sum(row_1.get(aggregationFieldsAlias), row_2.get(aggregationField)));
				} else if (aggregationType.equals(Aggregations.COUNT)) {
					newRow.put(aggregationFieldsAlias, AggregationService.count(row_1.get(aggregationFieldsAlias), row_2.get(aggregationField)));
				} else if (aggregationType.equals(Aggregations.COUNT_WITH_NULLS)) {
					newRow.put(aggregationFieldsAlias, AggregationService.countWithNulls(row_1.get(aggregationFieldsAlias)));
				} else if (aggregationType.equals(Aggregations.MIN)) {
					newRow.put(aggregationFieldsAlias, AggregationService.min(row_1.get(aggregationFieldsAlias), row_2.get(aggregationField)));
				} else if (aggregationType.equals(Aggregations.MAX)) {
					newRow.put(aggregationFieldsAlias, AggregationService.max(row_1.get(aggregationFieldsAlias), row_2.get(aggregationField)));
				} else if (aggregationType.equals(Aggregations.RANDOM)) {
					newRow.put(aggregationFieldsAlias, AggregationService.random(row_1.get(aggregationFieldsAlias), row_2.get(aggregationField)));
				} else if (aggregationType.equals(Aggregations.JOIN_BY_DELIM)) {
					newRow.put(aggregationFieldsAlias, AggregationService.joinByDelim(row_1.get(aggregationFieldsAlias), row_2.get(aggregationField),
					        groupByConfigBean.getJoinAggregationDelimeters().get(i)));
				} else {
					String[] averageFieldsNames = averageFields.get(aggregationFieldsAlias);
					newRow.put(averageFieldsNames[0], AggregationService.sum(row_1.get(averageFieldsNames[0]), row_2.get(aggregationField)));
					newRow.put(averageFieldsNames[1], AggregationService.count(row_1.get(averageFieldsNames[1]), row_2.get(aggregationField)));
				}
			}
		} catch (AggregationException aggregationException) {
			throw new DataCorruptedException(aggregationException);
		}
	}

	private void putGroupByRow(HashMap<String, Object> newRow, HashMap<String, Object> row_2) {
		GroupByConfigBean groupByConfigBean = (GroupByConfigBean) configBean;

		for (String groupByField : groupByConfigBean.getGroupByFields()) {
			newRow.put(groupByField, row_2.get(groupByField));
		}
	}
}
