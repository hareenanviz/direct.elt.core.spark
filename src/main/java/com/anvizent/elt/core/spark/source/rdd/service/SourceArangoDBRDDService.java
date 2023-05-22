package com.anvizent.elt.core.spark.source.rdd.service;

import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.spark.Partition;
import org.apache.spark.TaskContext;

import com.anvizent.elt.core.lib.exception.DateParseException;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidSituationException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedCoerceException;
import com.anvizent.elt.core.lib.util.TypeConversionUtil;
import com.anvizent.elt.core.listener.common.connection.ApplicationConnectionBean;
import com.anvizent.elt.core.listener.common.connection.ArangoDBConnectionByTaskId;
import com.anvizent.elt.core.spark.bean.SimplePartition;
import com.anvizent.elt.core.spark.constant.Constants.General;
import com.anvizent.elt.core.spark.source.config.bean.PartitionConfigBean;
import com.anvizent.elt.core.spark.source.config.bean.SourceArangoDBConfigBean;
import com.anvizent.query.builder.ArrangoDBQueryBuilder;
import com.anvizent.query.builder.QueryBuilder;
import com.anvizent.query.builder.exception.InvalidInputException;
import com.anvizent.query.builder.exception.UnderConstructionException;
import com.anvizent.universal.query.json.BatchDetails;
import com.anvizent.universal.query.json.DistinctRangeDetails;
import com.anvizent.universal.query.json.RangeDetails;
import com.anvizent.universal.query.json.SelectClause;
import com.anvizent.universal.query.json.SourceType;
import com.anvizent.universal.query.json.UniversalQueryJson;
import com.arangodb.ArangoCursor;

import scala.collection.Iterator;

/**
 * @author Hareen Bejjanki
 *
 */
public class SourceArangoDBRDDService {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static Iterator<LinkedHashMap> compute(Partition partition, SourceArangoDBConfigBean sourceArangoDBConfigBean) throws RecordProcessingException {
		try {
			UniversalQueryJson universalQueryJson = buildUniversalQueryJson(sourceArangoDBConfigBean);
			String query;
			ArrangoDBQueryBuilder arangoDBQueryBuilder = (ArrangoDBQueryBuilder) QueryBuilder.getQueryBuilder(SourceType.ARANGO_DB);
			String tempQuery = (String) arangoDBQueryBuilder.build(universalQueryJson);

			if (sourceArangoDBConfigBean.getPartitionType().equals(General.RANGE_PARTITION_TYPE)) {
				query = getRangePartitionQuery(sourceArangoDBConfigBean, tempQuery, (RangeDetails) partition);
			} else if (sourceArangoDBConfigBean.getPartitionType().equals(General.DISTINCT_FIELDS_PARTITION_TYPE)) {
				query = getDistinctFieldsPartitionQuery(sourceArangoDBConfigBean, tempQuery, (DistinctRangeDetails) partition);
			} else if (sourceArangoDBConfigBean.getPartitionType().equals(General.BATCH_PARTITION_TYPE)) {
				query = getBatchPartitionQuery(sourceArangoDBConfigBean, tempQuery, (BatchDetails) partition);
			} else {
				query = getSinglePartitionQuery(sourceArangoDBConfigBean, tempQuery);
			}

			ArangoCursor<LinkedHashMap> arangoCursor = (ArangoCursor<LinkedHashMap>) arangoDBQueryBuilder.execute(query, universalQueryJson,
			        ApplicationConnectionBean.getInstance()
			                .get(new ArangoDBConnectionByTaskId(sourceArangoDBConfigBean.getConnection(), null, TaskContext.getPartitionId()), true)[0],
			        LinkedHashMap.class);
			ArrayList<Map<String, Object>> rows = getRows(arangoCursor);

			return new ListOfMapIterator(rows, sourceArangoDBConfigBean.getStructType(), null);
		} catch (UnimplementedException | SQLException | InvalidInputException | UnderConstructionException | ParseException exception) {
			throw new RuntimeException(exception.getMessage(), exception);
		} catch (TimeoutException exception) {
			throw new RecordProcessingException(exception.getMessage(), exception);
		}
	}

	private static String getDistinctFieldsPartitionQuery(SourceArangoDBConfigBean sourceArangoDBConfigBean, String tempQuery, DistinctRangeDetails partition)
	        throws InvalidSituationException, UnsupportedCoerceException, UnderConstructionException, DateParseException {
		String query = "LET t1 = ( " + tempQuery + " ) FOR t2 IN t1 FILTER ";

		query += generateAndQuery(partition.getDistinctDataList().get(0));

		for (int j = 1; j < partition.getDistinctDataList().size(); j++) {
			query += " || " + generateAndQuery(partition.getDistinctDataList().get(j));
		}

		query += " RETURN t2";

		return query;
	}

	private static String generateAndQuery(Map<String, Object> distinctFields)
	        throws InvalidSituationException, UnsupportedCoerceException, UnderConstructionException, DateParseException {
		String query = "";
		int i = 0;
		for (String key : distinctFields.keySet()) {
			query += "t2." + key + " == " + getValue(distinctFields.get(key));

			if (i++ < distinctFields.size() - 1) {
				query += " && ";
			}
		}

		return query;
	}

	private static String getValue(Object value) throws UnderConstructionException, InvalidSituationException, UnsupportedCoerceException, DateParseException {
		if (value == null) {
			return null;
		} else if (isString(value.getClass().getName())) {
			return "\"" + value + "\"";
		} else if (isNumber(value.getClass().getName())) {
			return value + "";
		} else if (isDate(value.getClass().getName())) {
			return TypeConversionUtil.dateToOtherConversion((Date) value, Long.class, null) + "";
		} else {
			throw new UnderConstructionException("Field type '" + value.getClass().getName() + "' is not implemented.");
		}
	}

	private static boolean isDate(String className) {
		return className.equals(Date.class.getName());
	}

	private static boolean isNumber(String className) {
		return className.equals(Byte.class.getName()) || className.equals(Short.class.getName()) || className.equals(Integer.class.getName())
		        || className.equals(Long.class.getName()) || className.equals(Float.class.getName()) || className.equals(Double.class.getName())
		        || className.equals(BigDecimal.class.getName()) || className.equals(Boolean.class.getName());
	}

	private static boolean isString(String className) {
		return className.equals(String.class.getName()) || className.equals(Character.class.getName());
	}

	private static String getRangePartitionQuery(SourceArangoDBConfigBean sourceArangoDBConfigBean, String tempQuery, RangeDetails partition) {
		String query = "LET t1 = ( " + tempQuery + " ) FOR t2 IN t1";

		if (partition.getLowerBound() == null && partition.getUpperBound() != null) {
			query += " FILTER t2." + partition.getRangeField() + " <= " + partition.getUpperBound();
		} else if (partition.getLowerBound() != null && partition.getUpperBound() != null) {
			query += " FILTER t2." + partition.getRangeField() + " > " + partition.getLowerBound() + " && " + "t2." + partition.getRangeField() + " <= "
			        + partition.getUpperBound();
		} else if (partition.getLowerBound() != null && partition.getUpperBound() == null) {
			query += " FILTER t2." + partition.getRangeField() + " > " + partition.getLowerBound();
		}

		query += " RETURN t2";

		return query;
	}

	private static String getBatchPartitionQuery(SourceArangoDBConfigBean sourceArangoDBConfigBean, String tempQuery, BatchDetails partition) {
		String returnQuery = tempQuery.substring(tempQuery.lastIndexOf("RETURN"));
		String forQuery = tempQuery.substring(0, tempQuery.lastIndexOf("RETURN"));

		return forQuery + " LIMIT " + partition.getFrom() + ", " + partition.getTo() + " " + returnQuery;
	}

	private static String getSinglePartitionQuery(SourceArangoDBConfigBean sourceArangoDBConfigBean, String tempQuery) {
		if (sourceArangoDBConfigBean.getWhereClause() != null && !sourceArangoDBConfigBean.getWhereClause().isEmpty()) {
			String returnQuery = tempQuery.substring(tempQuery.lastIndexOf("RETURN"));
			String forQuery = tempQuery.substring(0, tempQuery.lastIndexOf("RETURN"));
			return forQuery + " " + sourceArangoDBConfigBean.getWhereClause() + " " + returnQuery;
		} else {
			return tempQuery;
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static ArrayList<Map<String, Object>> getRows(ArangoCursor<LinkedHashMap> arangoCursor) {
		ArrayList<Map<String, Object>> rows = new ArrayList<>();

		while (arangoCursor.hasNext()) {
			Map<String, Object> map = arangoCursor.next();
			rows.add(map);
		}

		return rows;
	}

	private static UniversalQueryJson buildUniversalQueryJson(SourceArangoDBConfigBean sourceArangoDBConfigBean) {
		UniversalQueryJson universalQueryJson = new UniversalQueryJson();
		universalQueryJson.setDbName(sourceArangoDBConfigBean.getConnection().getDBName());
		universalQueryJson.setTableName(sourceArangoDBConfigBean.getTableName());

		SelectClause selectClause = new SelectClause();
		selectClause.setSelectFields(sourceArangoDBConfigBean.getSelectFields());
		universalQueryJson.setSelectClause(selectClause);

		return universalQueryJson;
	}

	public static Partition[] generatePartitions(SourceArangoDBConfigBean sourceArangoDBConfigBean) throws RecordProcessingException {
		try {
			if (sourceArangoDBConfigBean.getPartitionType().equals(General.NO_PARTITION)
			        || (sourceArangoDBConfigBean.getPartitionConfigBean().getNumberOfPartitions() != null
			                && sourceArangoDBConfigBean.getPartitionConfigBean().getNumberOfPartitions() == 1)) {
				return singlePartition();
			} else if (sourceArangoDBConfigBean.getPartitionType().equals(General.RANGE_PARTITION_TYPE)) {
				return rangePartition(sourceArangoDBConfigBean.getPartitionConfigBean());
			} else if (sourceArangoDBConfigBean.getPartitionType().equals(General.DISTINCT_FIELDS_PARTITION_TYPE)) {
				return distinctFieldsPartition(sourceArangoDBConfigBean);
			} else {
				return batchPartition(sourceArangoDBConfigBean);
			}
		} catch (UnimplementedException | SQLException | ParseException | InvalidInputException | UnderConstructionException exception) {
			throw new RuntimeException(exception.getMessage(), exception);
		} catch (TimeoutException exception) {
			throw new RecordProcessingException(exception.getMessage(), exception);
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static Partition[] distinctFieldsPartition(SourceArangoDBConfigBean sourceArangoDBConfigBean)
	        throws ImproperValidationException, UnimplementedException, InvalidInputException, SQLException, TimeoutException {
		String query = generateDistinctFieldsPartitionQuery(sourceArangoDBConfigBean);
		UniversalQueryJson universalQueryJson = new UniversalQueryJson();
		universalQueryJson.setDbName(sourceArangoDBConfigBean.getConnection().getDBName());

		ArrangoDBQueryBuilder arangoDBQueryBuilder = (ArrangoDBQueryBuilder) QueryBuilder.getQueryBuilder(SourceType.ARANGO_DB);
		ArangoCursor<LinkedHashMap> groupedResult = (ArangoCursor<LinkedHashMap>) arangoDBQueryBuilder.execute(query, universalQueryJson,
		        ApplicationConnectionBean.getInstance()
		                .get(new ArangoDBConnectionByTaskId(sourceArangoDBConfigBean.getConnection(), null, TaskContext.getPartitionId()), true)[0],
		        LinkedHashMap.class);

		ArrayList<DistinctRangeDetails> distinctFieldsDetails = new ArrayList<>();

		while (groupedResult.hasNext()) {
			Map<String, Object> distinctData = groupedResult.next();
			DistinctRangeDetails distinctFieldDetails = new DistinctRangeDetails(BigDecimal.valueOf((Long) distinctData.remove("count")), distinctData);
			distinctFieldsDetails.add(distinctFieldDetails);
		}

		return generateDistictFieldPartitions(sourceArangoDBConfigBean.getPartitionConfigBean(), distinctFieldsDetails);
	}

	private static String generateDistinctFieldsPartitionQuery(SourceArangoDBConfigBean sourceArangoDBConfigBean) {
		String forQuery = "FOR t1 IN " + sourceArangoDBConfigBean.getTableName() + " RETURN t1";
		String query = "LET t1 = (" + forQuery + ") \nFOR t2 IN t1 \nCOLLECT ";

		for (int i = 0; i < sourceArangoDBConfigBean.getPartitionConfigBean().getPartitionColumns().size(); i++) {
			String partitionColumn = sourceArangoDBConfigBean.getPartitionConfigBean().getPartitionColumns().get(i);
			query += partitionColumn + " = " + "t2." + partitionColumn;

			if (i < sourceArangoDBConfigBean.getPartitionConfigBean().getPartitionColumns().size() - 1) {
				query += ", ";
			}
		}

		query += " WITH COUNT INTO count";
		query += "\nRETURN { ";
		for (int i = 0; i < sourceArangoDBConfigBean.getPartitionConfigBean().getPartitionColumns().size(); i++) {
			query += sourceArangoDBConfigBean.getPartitionConfigBean().getPartitionColumns().get(i);

			if (i < sourceArangoDBConfigBean.getPartitionConfigBean().getPartitionColumns().size() - 1) {
				query += ", ";
			}
		}
		query += ", count }";

		return query;
	}

	private static Partition[] generateDistictFieldPartitions(PartitionConfigBean partitionConfigBean, ArrayList<DistinctRangeDetails> distinctFieldsDetails) {
		ArrayList<DistinctRangeDetails> partitionedDistinctFieldsList = new ArrayList<>();

		if (distinctFieldsDetails.size() < partitionConfigBean.getNumberOfPartitions()) {
			for (int i = 0; i < distinctFieldsDetails.size(); i++) {
				distinctFieldsDetails.get(i).setIndex(i);
				partitionedDistinctFieldsList.add(distinctFieldsDetails.get(i));
			}
		} else {
			for (int i = 0; i < partitionConfigBean.getNumberOfPartitions(); i++) {
				distinctFieldsDetails.get(i).setIndex(i);
				partitionedDistinctFieldsList.add(distinctFieldsDetails.get(i));
			}

			for (int i = partitionConfigBean.getNumberOfPartitions(); i < distinctFieldsDetails.size(); i++) {
				Collections.sort(partitionedDistinctFieldsList);

				partitionedDistinctFieldsList.get(0).setCount(partitionedDistinctFieldsList.get(0).getCount().add(distinctFieldsDetails.get(i).getCount()));
				partitionedDistinctFieldsList.get(0).addDistinctDataList(distinctFieldsDetails.get(i).getDistinctDataList().get(0));
			}

			Collections.sort(partitionedDistinctFieldsList);
		}

		return generateDistictRangePartitions(partitionedDistinctFieldsList, partitionConfigBean.getNumberOfPartitions());
	}

	private static Partition[] generateDistictRangePartitions(ArrayList<DistinctRangeDetails> partitionedDistinctFieldsList, Integer numberOfPartitions) {
		Partition[] partitions = new Partition[numberOfPartitions];

		for (int i = 0; i < partitionedDistinctFieldsList.size(); i++) {
			partitions[partitionedDistinctFieldsList.get(i).index()] = getPartition(partitionedDistinctFieldsList.get(i));
		}

		return partitions;
	}

	private static Partition[] rangePartition(PartitionConfigBean partitionConfigBean) throws InvalidInputException {
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

	private static Partition getPartition(DistinctRangeDetails distinctRangeDetails) {
		return new SimplePartition(distinctRangeDetails.index());
	}

	private static Partition getPartition(RangeDetails rangeDetails) {
		return new SimplePartition(rangeDetails.index());
	}

	private static Partition getPartition(BatchDetails batchDetails) {
		return new SimplePartition(batchDetails.index());
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

	@SuppressWarnings("unchecked")
	private static Partition[] batchPartition(SourceArangoDBConfigBean sourceArangoDBConfigBean) throws InvalidInputException, UnderConstructionException,
	        ParseException, ImproperValidationException, UnimplementedException, SQLException, TimeoutException {
		UniversalQueryJson universalQueryJson = new UniversalQueryJson();
		universalQueryJson.setDbName(sourceArangoDBConfigBean.getConnection().getDBName());
		universalQueryJson.setTableName(sourceArangoDBConfigBean.getTableName());
		universalQueryJson.setCount(true);

		ArrangoDBQueryBuilder arangoDBQueryBuilder = (ArrangoDBQueryBuilder) QueryBuilder.getQueryBuilder(SourceType.ARANGO_DB);
		String query = (String) arangoDBQueryBuilder.build(universalQueryJson);

		ArangoCursor<Long> arangoCursor = (ArangoCursor<Long>) arangoDBQueryBuilder.execute(query, universalQueryJson,
		        ApplicationConnectionBean.getInstance()
		                .get(new ArangoDBConnectionByTaskId(sourceArangoDBConfigBean.getConnection(), null, TaskContext.getPartitionId()), true)[0],
		        Long.class);
		long count = arangoCursor.next();

		int numberOfPartitions = count == 0 ? 1 : (int) Math.ceil((count / (double) sourceArangoDBConfigBean.getPartitionSize()));

		Partition[] partitions = new Partition[numberOfPartitions];
		for (int i = 0, index = 0; i < count; i += sourceArangoDBConfigBean.getPartitionSize(), index++) {
			partitions[index] = getPartition(new BatchDetails(index, (long) i, sourceArangoDBConfigBean.getPartitionSize()));
		}

		return partitions;
	}

	private static Partition[] singlePartition() {
		Partition[] partitions = new Partition[1];
		partitions[0] = getPartition(new RangeDetails(0, null, null, null));

		return partitions;
	}

}
