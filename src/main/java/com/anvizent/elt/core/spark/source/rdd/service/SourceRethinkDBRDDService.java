package com.anvizent.elt.core.spark.source.rdd.service;

import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.spark.Partition;
import org.apache.spark.TaskContext;

import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.listener.common.connection.ApplicationConnectionBean;
import com.anvizent.elt.core.listener.common.connection.RethinkDBConnectionByTaskId;
import com.anvizent.elt.core.spark.bean.SimplePartition;
import com.anvizent.elt.core.spark.constant.Constants.General;
import com.anvizent.elt.core.spark.source.config.bean.PartitionConfigBean;
import com.anvizent.elt.core.spark.source.config.bean.SourceRethinkDBConfigBean;
import com.anvizent.query.builder.QueryBuilder;
import com.anvizent.query.builder.RethinkDBQueryBuilder;
import com.anvizent.query.builder.exception.InvalidInputException;
import com.anvizent.query.builder.exception.UnderConstructionException;
import com.anvizent.universal.query.json.DistinctRangeDetails;
import com.anvizent.universal.query.json.GroupByClause;
import com.anvizent.universal.query.json.Pagination;
import com.anvizent.universal.query.json.RangeDetails;
import com.anvizent.universal.query.json.SelectClause;
import com.anvizent.universal.query.json.SourceType;
import com.anvizent.universal.query.json.UniversalQueryJson;
import com.anvizent.universal.query.json.WhereClause;
import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.model.GroupedResult;
import com.rethinkdb.net.Cursor;

import scala.collection.Iterator;

/**
 * @author Hareen Bejjanki
 *
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class SourceRethinkDBRDDService {

	public static Iterator<LinkedHashMap> compute(Partition partition, SourceRethinkDBConfigBean rethinkDBConfigBean, UniversalQueryJson universalQueryJson)
	        throws RecordProcessingException {
		try {
			if (rethinkDBConfigBean.getPartitionType().equals(General.DISTINCT_FIELDS_PARTITION_TYPE)) {
				addDistinctRangePartitionWhereClause(partition, universalQueryJson);
			} else if (rethinkDBConfigBean.getPartitionType().equals(General.RANGE_PARTITION_TYPE)) {
				addRangePartitionWhereClause(partition, universalQueryJson);
			} else if (rethinkDBConfigBean.getPartitionType().equals(General.BATCH_PARTITION_TYPE)) {
				addBatchPartitionClause(partition, universalQueryJson);
			}

			RethinkDBQueryBuilder rethinkDBQueryBuilder = (RethinkDBQueryBuilder) QueryBuilder.getQueryBuilder(SourceType.RETHINK_DB);

			ReqlExpr reqlExpr = (ReqlExpr) rethinkDBQueryBuilder.build(universalQueryJson);

			Cursor<? extends Map<String, Object>> result = (Cursor<? extends Map<String, Object>>) rethinkDBQueryBuilder
			        .execute(reqlExpr, null,
			                ApplicationConnectionBean.getInstance()
			                        .get(new RethinkDBConnectionByTaskId(rethinkDBConfigBean.getConnection(), null, TaskContext.getPartitionId()), true)[0],
			                null);

			ArrayList<Map<String, Object>> list = getResultList(result);

			return new ListOfMapIterator(list, rethinkDBConfigBean.getStructType(), rethinkDBConfigBean.getTimeZoneOffset());
		} catch (ImproperValidationException | UnimplementedException | SQLException | InvalidInputException | UnderConstructionException exception) {
			throw new RuntimeException(exception.getMessage(), exception);
		} catch (TimeoutException exception) {
			throw new RecordProcessingException(exception.getMessage(), exception);
		}
	}

	private static void addBatchPartitionClause(Partition partition, UniversalQueryJson universalQueryJson) {
		universalQueryJson.setPagination((Pagination) partition);
	}

	private static void addRangePartitionWhereClause(Partition partition, UniversalQueryJson universalQueryJson) {
		universalQueryJson.getWhereClause().addRangeDetails((RangeDetails) partition);
	}

	private static void addDistinctRangePartitionWhereClause(Partition partition, UniversalQueryJson universalQueryJson) {
		universalQueryJson.getWhereClause().addDistinctRangeDetails((DistinctRangeDetails) partition);
	}

	public static ArrayList<Map<String, Object>> getResultList(Cursor<? extends Map<String, Object>> result) {
		ArrayList<Map<String, Object>> list = new ArrayList<>();

		while (result.hasNext()) {
			list.add(result.next());
		}

		return list;
	}

	public static UniversalQueryJson buildUniversalQueryJson(SourceRethinkDBConfigBean rethinkDBConfigBean) {
		UniversalQueryJson universalQueryJson = new UniversalQueryJson();

		universalQueryJson.setTableName(rethinkDBConfigBean.getTableName());

		SelectClause selectClause = new SelectClause();
		selectClause.setSelectFields(rethinkDBConfigBean.getSelectFields());
		universalQueryJson.setSelectClause(selectClause);

		universalQueryJson.setWhereClause(new WhereClause());

		if (rethinkDBConfigBean.getPartitionType().equals(General.DISTINCT_FIELDS_PARTITION_TYPE)) {
			GroupByClause groupByClause = new GroupByClause();
			groupByClause.setGroupByFields(rethinkDBConfigBean.getPartitionConfigBean().getPartitionColumns());
			universalQueryJson.setGroupByClause(groupByClause);
		}

		// universalQueryJson.setLimit(rethinkDBConfigBean.getLimit());

		return universalQueryJson;
	}

	public static Partition[] generatePartitions(SourceRethinkDBConfigBean rethinkDBConfigBean, UniversalQueryJson universalQueryJson)
	        throws RecordProcessingException {
		try {
			if (rethinkDBConfigBean.getPartitionType().equals(General.NO_PARTITION)
			        || (rethinkDBConfigBean.getPartitionConfigBean().getNumberOfPartitions() != null
			                && rethinkDBConfigBean.getPartitionConfigBean().getNumberOfPartitions() == 1)) {
				return singlePartition();
			} else if (rethinkDBConfigBean.getPartitionType().equals(General.RANGE_PARTITION_TYPE)) {
				return rangePartitions(rethinkDBConfigBean.getPartitionConfigBean());
			} else if (rethinkDBConfigBean.getPartitionType().equals(General.DISTINCT_FIELDS_PARTITION_TYPE)) {
				return distinctRangePartitions(rethinkDBConfigBean, rethinkDBConfigBean.getPartitionConfigBean(), universalQueryJson);
			} else {
				return batchPartitions(rethinkDBConfigBean);
			}
		} catch (ImproperValidationException | UnimplementedException | SQLException | InvalidInputException | UnderConstructionException exception) {
			throw new RuntimeException(exception.getMessage(), exception);
		} catch (TimeoutException exception) {
			throw new RecordProcessingException(exception.getMessage(), exception);
		}
	}

	private static Partition[] singlePartition() {
		Partition[] partitions = new Partition[1];
		partitions[0] = getPartition(new RangeDetails(0, null, null, null));

		return partitions;
	}

	private static Partition[] rangePartitions(PartitionConfigBean partitionConfigBean) throws InvalidInputException {
		Partition[] partitions = new Partition[partitionConfigBean.getNumberOfPartitions()];

		double delta = getDelta(partitionConfigBean);

		validatePossibleNumberOfPartitions(delta, partitionConfigBean);

		BigDecimal currentLowerBound = null;
		BigDecimal nextUpperBound = partitionConfigBean.getLowerBound().add(BigDecimal.valueOf(delta));

		partitions[0] = getPartition(new RangeDetails(0, partitionConfigBean.getPartitionColumns().get(0), null, nextUpperBound));

		for (int i = 1; i < partitionConfigBean.getNumberOfPartitions() - 1; i++) {
			currentLowerBound = nextUpperBound;
			nextUpperBound = nextUpperBound.add(BigDecimal.valueOf(delta));
			partitions[i] = getPartition(new RangeDetails(i, partitionConfigBean.getPartitionColumns().get(0), currentLowerBound, nextUpperBound));
		}

		partitions[partitionConfigBean.getNumberOfPartitions() - 1] = getPartition(
		        new RangeDetails(partitionConfigBean.getNumberOfPartitions() - 1, partitionConfigBean.getPartitionColumns().get(0), nextUpperBound, null));

		return partitions;
	}

	private static Partition[] distinctRangePartitions(SourceRethinkDBConfigBean rethinkDBConfigBean, PartitionConfigBean partitionConfigBean,
	        UniversalQueryJson universalQueryJson)
	        throws InvalidInputException, UnderConstructionException, ImproperValidationException, UnimplementedException, SQLException, TimeoutException {

		RethinkDBQueryBuilder rethinkDBQueryBuilder = (RethinkDBQueryBuilder) QueryBuilder.getQueryBuilder(SourceType.RETHINK_DB);
		ReqlExpr reqlExpr = (ReqlExpr) rethinkDBQueryBuilder.build(universalQueryJson);

		ArrayList<GroupedResult> groupedResults = (ArrayList<GroupedResult>) rethinkDBQueryBuilder.execute(reqlExpr, null, ApplicationConnectionBean
		        .getInstance().get(new RethinkDBConnectionByTaskId(rethinkDBConfigBean.getConnection(), null, TaskContext.getPartitionId()), true)[0], null);

		resetGroupByClause(rethinkDBConfigBean, universalQueryJson);

		ArrayList<DistinctRangeDetails> distinctRangeDetailsList = new ArrayList<>();

		for (int i = 0; i < groupedResults.size(); i++) {
			ArrayList<Object> values = (ArrayList<Object>) groupedResults.get(i).values;
			Map<String, Object> group = (Map<String, Object>) groupedResults.get(i).group;

			DistinctRangeDetails distinctRangeDetails = new DistinctRangeDetails(BigDecimal.valueOf((Long) values.get(0)), group);
			distinctRangeDetailsList.add(distinctRangeDetails);
		}

		Collections.sort(distinctRangeDetailsList, Collections.reverseOrder());

		return generateDistictRangePartitions(partitionConfigBean, distinctRangeDetailsList);
	}

	private static Partition[] batchPartitions(SourceRethinkDBConfigBean rethinkDBConfigBean)
	        throws InvalidInputException, UnderConstructionException, ImproperValidationException, UnimplementedException, SQLException, TimeoutException {
		UniversalQueryJson universalQueryJson = new UniversalQueryJson();
		universalQueryJson.setTableName(rethinkDBConfigBean.getTableName());
		universalQueryJson.setCount(true);

		RethinkDBQueryBuilder rethinkDBQueryBuilder = (RethinkDBQueryBuilder) QueryBuilder.getQueryBuilder(SourceType.RETHINK_DB);
		ReqlExpr reqlExpr = (ReqlExpr) rethinkDBQueryBuilder.build(universalQueryJson);
		Long count = (Long) rethinkDBQueryBuilder.execute(reqlExpr, null, ApplicationConnectionBean.getInstance()
		        .get(new RethinkDBConnectionByTaskId(rethinkDBConfigBean.getConnection(), null, TaskContext.getPartitionId()), true)[0], null);

		int numberOfPartitions = count == 0 ? 1 : (int) Math.ceil((count / (double) rethinkDBConfigBean.getPartitionSize()));

		Partition[] partitions = new Partition[numberOfPartitions];

		for (int i = 0; i < partitions.length; i++) {
			partitions[i] = getPartition(new Pagination(i, rethinkDBConfigBean.getPartitionSize(), (long) i));
		}

		return partitions;
	}

	private static void resetGroupByClause(SourceRethinkDBConfigBean rethinkDBConfigBean, UniversalQueryJson universalQueryJson) {
		universalQueryJson.setGroupByClause(null);
	}

	private static Partition[] generateDistictRangePartitions(PartitionConfigBean partitionConfigBean,
	        ArrayList<DistinctRangeDetails> distinctRangeDetailsList) {
		ArrayList<DistinctRangeDetails> partitionedDistinctRangeList = new ArrayList<>();

		if (distinctRangeDetailsList.size() < partitionConfigBean.getNumberOfPartitions()) {
			for (int i = 0; i < distinctRangeDetailsList.size(); i++) {
				distinctRangeDetailsList.get(i).setIndex(i);
				partitionedDistinctRangeList.add(distinctRangeDetailsList.get(i));
			}

			return generateDistictRangePartitions(partitionedDistinctRangeList, distinctRangeDetailsList.size());
		} else {
			for (int i = 0; i < partitionConfigBean.getNumberOfPartitions(); i++) {
				distinctRangeDetailsList.get(i).setIndex(i);
				partitionedDistinctRangeList.add(distinctRangeDetailsList.get(i));
			}

			for (int i = partitionConfigBean.getNumberOfPartitions(); i < distinctRangeDetailsList.size(); i++) {
				Collections.sort(partitionedDistinctRangeList);

				partitionedDistinctRangeList.get(0).setCount(partitionedDistinctRangeList.get(0).getCount().add(distinctRangeDetailsList.get(i).getCount()));
				partitionedDistinctRangeList.get(0).addDistinctDataList(distinctRangeDetailsList.get(i).getDistinctDataList().get(0));
			}

			return generateDistictRangePartitions(partitionedDistinctRangeList, partitionConfigBean.getNumberOfPartitions());
		}
	}

	private static Partition[] generateDistictRangePartitions(ArrayList<DistinctRangeDetails> partitionedDistinctRangeList, Integer numberOfPartitions) {

		Partition[] partitions = new Partition[numberOfPartitions];

		for (int i = 0; i < partitionedDistinctRangeList.size(); i++) {
			partitions[partitionedDistinctRangeList.get(i).index()] = getPartition(partitionedDistinctRangeList.get(i));
		}

		return partitions;
	}

	private static Partition getPartition(DistinctRangeDetails distinctRangeDetails) {
		return new SimplePartition(distinctRangeDetails.index());
	}

	private static Partition getPartition(RangeDetails rangeDetails) {
		return new SimplePartition(rangeDetails.index());
	}

	private static Partition getPartition(Pagination pagination) {
		return new SimplePartition(pagination.index());
	}

	private static double getDelta(PartitionConfigBean partitionConfigBean) {
		return Math.ceil(((partitionConfigBean.getUpperBound().subtract(partitionConfigBean.getLowerBound()))
		        .divide(BigDecimal.valueOf(partitionConfigBean.getNumberOfPartitions()), MathContext.DECIMAL128)).doubleValue());
	}

	private static void validatePossibleNumberOfPartitions(double delta, PartitionConfigBean partitionConfigBean) throws InvalidInputException {
		Integer possibleNumberOfPartitions = 0;

		for (BigDecimal i = partitionConfigBean.getLowerBound(); i.compareTo(partitionConfigBean.getUpperBound()) < 0; i = i.add(BigDecimal.valueOf(delta))) {
			possibleNumberOfPartitions = possibleNumberOfPartitions + 1;
		}

		if (partitionConfigBean.getNumberOfPartitions() != possibleNumberOfPartitions) {
			throw new InvalidInputException("Invalid number of partitons are provided for lower and upper bound. Possible partitions: "
			        + possibleNumberOfPartitions + " but provided partitions: " + partitionConfigBean.getNumberOfPartitions());
		}
	}
}
