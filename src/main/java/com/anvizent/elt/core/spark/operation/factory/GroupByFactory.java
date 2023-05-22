package com.anvizent.elt.core.spark.operation.factory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.constant.Constants.StatsNames;
import com.anvizent.elt.core.lib.constant.StatsCategory;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.lib.stats.calculator.AnvizentMapToPairStatsCalculator;
import com.anvizent.elt.core.lib.stats.calculator.AnvizentStatsCalculator;
import com.anvizent.elt.core.lib.stats.calculator.IStatsCalculator;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.listener.common.constant.StatsType;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.common.util.ErrorHandlerUtil;
import com.anvizent.elt.core.spark.constant.Aggregations;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.Components;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.GroupBy;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.AggregationException;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.operation.config.bean.GroupByConfigBean;
import com.anvizent.elt.core.spark.operation.doc.helper.GroupByDocHelper;
import com.anvizent.elt.core.spark.operation.function.AggregateCombinerFunction;
import com.anvizent.elt.core.spark.operation.function.AggregateSequenceFunction;
import com.anvizent.elt.core.spark.operation.function.AverageFunction;
import com.anvizent.elt.core.spark.operation.function.MapToPairGroupByFunction;
import com.anvizent.elt.core.spark.operation.function.PairToNormalRDDFunction;
import com.anvizent.elt.core.spark.operation.service.AggregationService;
import com.anvizent.elt.core.spark.operation.validator.GroupByValidator;
import com.anvizent.elt.core.spark.schema.validator.SchemaValidator;
import com.anvizent.elt.core.spark.util.StructureUtil;
import com.anvizent.elt.core.spark.validator.ResourceValidator;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
@SuppressWarnings("rawtypes")
public class GroupByFactory extends SimpleOperationFactory {

	private static final long serialVersionUID = 1L;

	@Override
	protected Component process(ConfigBean configBean, Component component, ErrorHandlerSink errorHandlerSink) throws Exception {
		GroupByConfigBean groupByConfigBean = (GroupByConfigBean) configBean;

		LinkedHashMap<String, AnvizentDataType> newStructure = getNewStructure(groupByConfigBean, component.getStructure());

		LinkedHashMap<String, Object> zeroValues = new LinkedHashMap<>();

		HashMap<String, String[]> averageFields = getAverageFields(groupByConfigBean, groupByConfigBean.getAggregationFields(),
		        groupByConfigBean.getAggregations(), groupByConfigBean.getAliasNames(), component.getStructure(), zeroValues);

		JavaPairRDD<HashMap<String, Object>, HashMap<String, Object>> groupedPairRDD = groupBy(component, groupByConfigBean, newStructure, errorHandlerSink);
		JavaPairRDD<HashMap<String, Object>, HashMap<String, Object>> aggregateRDD = aggregation(groupedPairRDD, component, groupByConfigBean, newStructure,
		        averageFields, zeroValues, errorHandlerSink);
		JavaRDD<HashMap<String, Object>> finalRDD = getFinalRDD(averageFields, groupByConfigBean, component, newStructure, aggregateRDD, errorHandlerSink);

		return Component.createComponent(component.getSparkSession(), groupByConfigBean.getName(), finalRDD, newStructure);
	}

	private LinkedHashMap<String, AnvizentDataType> getNewStructure(GroupByConfigBean groupByConfigBean, LinkedHashMap<String, AnvizentDataType> structure)
	        throws InvalidConfigException, UnsupportedException {

		return StructureUtil.getNewStructure(groupByConfigBean.getConfigName(), groupByConfigBean.getName(), groupByConfigBean.getSeekDetails(),
		        new LinkedHashMap<String, AnvizentDataType>(structure), groupByConfigBean.getAliasNames(),
		        getNewAnvizentDataTypes(groupByConfigBean, structure), groupByConfigBean.getAggregationFieldPositions(), groupByConfigBean.getGroupByFields(),
		        groupByConfigBean.getGroupByFieldPositions(), GroupBy.AGGREGATION_FIELD_ALIAS_NAMES, GroupBy.GROUP_BY_FIELDS,
		        GroupBy.AGGREGATION_FIELDS_POSITIONS, GroupBy.GROUP_BY_FIELDS_POSITIONS, true);
	}

	private ArrayList<AnvizentDataType> getNewAnvizentDataTypes(GroupByConfigBean groupByConfigBean, LinkedHashMap<String, AnvizentDataType> structure)
	        throws UnsupportedException {
		ArrayList<AnvizentDataType> newAnvizentDataTypes = new ArrayList<>();

		for (int i = 0; i < groupByConfigBean.getAggregations().size(); i++) {
			Aggregations aggregation = groupByConfigBean.getAggregations().get(i);
			if (aggregation.equals(Aggregations.MAX) || aggregation.equals(Aggregations.MIN) || aggregation.equals(Aggregations.SUM)
			        || aggregation.equals(Aggregations.RANDOM)) {
				newAnvizentDataTypes.add(structure.get(groupByConfigBean.getAggregationFields().get(i)));
			} else if (aggregation.equals(Aggregations.COUNT) || aggregation.equals(Aggregations.COUNT_WITH_NULLS)) {
				newAnvizentDataTypes.add(new AnvizentDataType(Long.class));
			} else if (aggregation.equals(Aggregations.JOIN_BY_DELIM)) {
				newAnvizentDataTypes.add(new AnvizentDataType(String.class));
			} else {
				int precision = getPrecision(groupByConfigBean.getPrecisions(), i);
				int scale = getScale(groupByConfigBean.getScales(), i);
				newAnvizentDataTypes.add(new AnvizentDataType(BigDecimal.class, precision, scale));
			}
		}

		return newAnvizentDataTypes;
	}

	private int getScale(ArrayList<Integer> scales, int index) {
		return (scales == null || scales.isEmpty()) ? General.DECIMAL_SCALE : (scales.get(index) == null ? General.DECIMAL_SCALE : scales.get(index));
	}

	private int getPrecision(ArrayList<Integer> precisions, int index) {
		return (precisions == null || precisions.isEmpty()) ? General.DECIMAL_PRECISION
		        : (precisions.get(index) == null ? General.DECIMAL_PRECISION : precisions.get(index));
	}

	private LinkedHashMap<String, Object> getZeroValues(GroupByConfigBean groupByConfigBean, LinkedHashMap<String, AnvizentDataType> structure,
	        LinkedHashMap<String, Object> zeroValues) throws AggregationException, UnsupportedException, InvalidConfigException {

		for (int i = 0; i < groupByConfigBean.getAggregationFields().size(); i++) {
			String aggregationField = groupByConfigBean.getAggregationFields().get(i);
			Class javaType = getJavaType(groupByConfigBean, structure, aggregationField);

			putZeroValues(zeroValues, aggregationField, groupByConfigBean.getAliasNames().get(i), groupByConfigBean.getAggregations().get(i), javaType);
		}

		return zeroValues;
	}

	private static Class getJavaType(GroupByConfigBean groupByConfigBean, LinkedHashMap<String, AnvizentDataType> structure, String aggregationField)
	        throws InvalidConfigException {
		AnvizentDataType anvizentDataType = structure.get(aggregationField);

		if (anvizentDataType == null) {
			InvalidConfigException invalidConfigException = new InvalidConfigException();

			invalidConfigException.setComponent(groupByConfigBean.getConfigName());
			invalidConfigException.setComponentName(groupByConfigBean.getName());
			invalidConfigException.setSeekDetails(groupByConfigBean.getSeekDetails());

			invalidConfigException.add("Aggregation field ''{0}'' is not found in the source!", aggregationField);

			throw invalidConfigException;
		}

		return anvizentDataType.getJavaType();
	}

	private void putZeroValues(LinkedHashMap<String, Object> zeroValues, String aggregationField, String aggregationFieldAlias, Aggregations aggregationType,
	        Class javaType) throws AggregationException {
		if (aggregationType.equals(Aggregations.SUM)) {
			zeroValues.put(aggregationFieldAlias, AggregationService.getSumZeroValue(aggregationField, javaType));
		} else if (aggregationType.equals(Aggregations.COUNT) || aggregationType.equals(Aggregations.COUNT_WITH_NULLS)) {
			zeroValues.put(aggregationFieldAlias, 0L);
		} else if (aggregationType.equals(Aggregations.MIN)) {
			zeroValues.put(aggregationFieldAlias, AggregationService.getMinZeroValue(aggregationField, javaType));
		} else if (aggregationType.equals(Aggregations.MAX)) {
			zeroValues.put(aggregationFieldAlias, AggregationService.getMaxZeroValue(aggregationField, javaType));
		} else if (aggregationType.equals(Aggregations.RANDOM) || aggregationType.equals(Aggregations.JOIN_BY_DELIM)) {
			zeroValues.put(aggregationFieldAlias, null);
		}
	}

	private static HashMap<String, String[]> getAverageFields(GroupByConfigBean groupByConfigBean, ArrayList<String> aggregationFields,
	        ArrayList<Aggregations> aggregationTypes, ArrayList<String> aggregationFieldAliases, LinkedHashMap<String, AnvizentDataType> structure,
	        LinkedHashMap<String, Object> zeroValues) throws AggregationException, InvalidConfigException {
		HashMap<String, String[]> averageFields = new HashMap<String, String[]>();

		for (int i = 0; i < aggregationTypes.size(); i++) {
			String aggregationField = aggregationFields.get(i);
			Class javaType = getJavaType(groupByConfigBean, structure, aggregationField);

			createAndVerifyAverageFieldNames(averageFields, structure, aggregationField, aggregationFieldAliases, aggregationTypes, zeroValues, javaType, i);
		}

		return averageFields;
	}

	private static void createAndVerifyAverageFieldNames(HashMap<String, String[]> averageFields, LinkedHashMap<String, AnvizentDataType> structure,
	        String aggregationField, ArrayList<String> aggregationFieldAliases, ArrayList<Aggregations> aggregationTypes,
	        LinkedHashMap<String, Object> zeroValues, Class javaType, int i) throws AggregationException {
		String averageSumField = "", averageCountField = "";

		if (aggregationTypes.get(i).equals(Aggregations.AVG)) {
			String aggregationFieldAlias = aggregationFieldAliases.get(i);
			averageSumField += aggregationFieldAlias + General.AVERAGE_SUM_POST_FIX;
			averageCountField += aggregationFieldAlias + General.AVERAGE_COUNT_POST_FIX;

			averageSumField = verifyAverageFieldNames(averageSumField, aggregationFieldAliases, structure, General.AVERAGE_SUM_POST_FIX);
			averageCountField = verifyAverageFieldNames(averageCountField, aggregationFieldAliases, structure, General.AVERAGE_COUNT_POST_FIX);

			zeroValues.put(averageSumField, AggregationService.getSumZeroValue(aggregationField, javaType));
			zeroValues.put(averageCountField, 0L);

			averageFields.put(aggregationFieldAlias, new String[] { averageSumField, averageCountField });
		}
	}

	private static String verifyAverageFieldNames(String averageField, ArrayList<String> schemaFields, LinkedHashMap<String, AnvizentDataType> structure,
	        String postFix) {
		if (schemaFields.contains(averageField) || structure.containsKey(averageField)) {
			averageField += postFix;
			verifyAverageFieldNames(averageField, schemaFields, structure, postFix);
		}

		return averageField;
	}

	private JavaPairRDD<HashMap<String, Object>, HashMap<String, Object>> groupBy(Component component, GroupByConfigBean groupByConfigBean,
	        LinkedHashMap<String, AnvizentDataType> newStructure, ErrorHandlerSink errorHandlerSink)
	        throws UnimplementedException, ImproperValidationException, InvalidRelationException, UnsupportedException, Exception {

		JavaPairRDD<HashMap<String, Object>, HashMap<String, Object>> groupedPairRDD = component.getRDD(groupByConfigBean.getSourceStream())
		        .mapToPair(new MapToPairGroupByFunction(groupByConfigBean, component.getStructure(), newStructure,
		                ApplicationBean.getInstance().getAccumulators(groupByConfigBean.getName(), getName()),
		                ErrorHandlerUtil.getErrorHandlerFunction(groupByConfigBean, component.getStructure(), errorHandlerSink, getName()),
		                ErrorHandlerUtil.getJobDetails(groupByConfigBean, getName())));
		if (groupByConfigBean.isPersist()) {
			groupedPairRDD.persist(StorageLevel.MEMORY_AND_DISK());
		}

		return groupedPairRDD;
	}

	private JavaPairRDD<HashMap<String, Object>, HashMap<String, Object>> aggregation(
	        JavaPairRDD<HashMap<String, Object>, HashMap<String, Object>> groupedPairRDD, Component component, GroupByConfigBean groupByConfigBean,
	        LinkedHashMap<String, AnvizentDataType> newStructure, HashMap<String, String[]> averageFields, LinkedHashMap<String, Object> zeroValues,
	        ErrorHandlerSink errorHandlerSink) throws ImproperValidationException, InvalidRelationException, Exception {

		JavaPairRDD<HashMap<String, Object>, HashMap<String, Object>> aggregateRDD = groupedPairRDD.aggregateByKey(
		        getZeroValues(groupByConfigBean, component.getStructure(), zeroValues),
		        new AggregateSequenceFunction(groupByConfigBean, component.getStructure(), newStructure, averageFields,
		                ErrorHandlerUtil.getErrorHandlerFunction(groupByConfigBean, component.getStructure(), errorHandlerSink,
		                        Components.AGGREGATION.get(General.NAME) + "_Sequence"),
		                ErrorHandlerUtil.getJobDetails(groupByConfigBean, Components.AGGREGATION.get(General.NAME) + "_Sequence")),
		        new AggregateCombinerFunction(groupByConfigBean, component.getStructure(), newStructure, averageFields,
		                ErrorHandlerUtil.getErrorHandlerFunction(groupByConfigBean, component.getStructure(), errorHandlerSink,
		                        Components.AGGREGATION.get(General.NAME) + "_Combiner"),
		                ErrorHandlerUtil.getJobDetails(groupByConfigBean, Components.AGGREGATION.get(General.NAME) + "_Combiner")));

		if (groupByConfigBean.isPersist()) {
			aggregateRDD.persist(StorageLevel.MEMORY_AND_DISK());
		}

		return aggregateRDD;
	}

	private JavaRDD<HashMap<String, Object>> getFinalRDD(HashMap<String, String[]> averageFields, GroupByConfigBean groupByConfigBean, Component component,
	        LinkedHashMap<String, AnvizentDataType> newStructure, JavaPairRDD<HashMap<String, Object>, HashMap<String, Object>> aggregateRDD,
	        ErrorHandlerSink errorHandlerSink)
	        throws UnimplementedException, ImproperValidationException, InvalidRelationException, UnsupportedException, Exception {

		JavaRDD<HashMap<String, Object>> finalRDD;

		if (averageFields.size() > 0) {
			finalRDD = aggregateRDD.values().flatMap(new AverageFunction(groupByConfigBean, component.getStructure(), newStructure, averageFields,
			        ApplicationBean.getInstance().getAccumulators(groupByConfigBean.getName(), Components.AVERAGE.get(General.NAME)), ErrorHandlerUtil
			                .getErrorHandlerFunction(groupByConfigBean, component.getStructure(), errorHandlerSink, Components.AVERAGE.get(General.NAME)),
			        ErrorHandlerUtil.getJobDetails(groupByConfigBean, Components.AVERAGE.get(General.NAME))));
		} else {
			finalRDD = aggregateRDD.flatMap(new PairToNormalRDDFunction(groupByConfigBean, component.getStructure(), newStructure,
			        ApplicationBean.getInstance().getAccumulators(groupByConfigBean.getName(), Components.AGGREGATION.get(General.NAME)),
			        ErrorHandlerUtil.getErrorHandlerFunction(groupByConfigBean, component.getStructure(), errorHandlerSink,
			                Components.AGGREGATION.get(General.NAME)),
			        ErrorHandlerUtil.getJobDetails(groupByConfigBean, Components.AGGREGATION.get(General.NAME))));
		}

		if (groupByConfigBean.isPersist()) {
			finalRDD.persist(StorageLevel.MEMORY_AND_DISK());
		}

		return finalRDD;
	}

	@Override
	public String getName() {
		return Components.GROUP_BY.get(General.NAME);
	}

	@Override
	public DocHelper getDocHelper() throws InvalidParameter {
		return new GroupByDocHelper(this);
	}

	@Override
	public Validator getValidator() {
		return new GroupByValidator(this);
	}

	@Override
	public SchemaValidator getSchemaValidator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Integer getMaxInputs() {
		return 1;
	}

	@Override
	public Integer getMinInputs() {
		return 1;
	}

	@Override
	protected IStatsCalculator getStatsCalculator(StatsCategory statsCategory, String statsName, String internalRDDName) {
		if (internalRDDName.equals(getName())) {
			return new AnvizentMapToPairStatsCalculator<LinkedHashMap<String, Object>, LinkedHashMap<String, Object>>(statsCategory, statsName);
		} else {
			return new AnvizentStatsCalculator<LinkedHashMap<String, Object>, LinkedHashMap<String, Object>>(statsCategory, statsName);
		}
	}

	@Override
	protected void createFactoryAccumulators(ConfigBean configBean, StatsType statsType, boolean componentLevel) {
		AnvizentAccumulator inAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(), getName(),
		        StatsCategory.IN, StatsNames.IN, true, getStatsCalculator(StatsCategory.IN, StatsNames.IN, getName()));

		ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName(), inAnvizentAccumulator);

		if (((GroupByConfigBean) configBean).getAggregations().contains(Aggregations.AVG)) {
			createAverageAccumulators((GroupByConfigBean) configBean, statsType, componentLevel);
		} else {
			createAggregationAccumulators((GroupByConfigBean) configBean, statsType, componentLevel);
		}
	}

	private void createAverageAccumulators(GroupByConfigBean groupByConfigBean, StatsType statsType, boolean componentLevel) {
		if ((statsType.equals(StatsType.COMPONENT_LEVEL) && !componentLevel) || statsType.equals(StatsType.ALL)) {
			AnvizentAccumulator outAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), groupByConfigBean.getName(),
			        Components.AVERAGE.get(General.NAME), StatsCategory.OUT, StatsNames.OUT, !componentLevel,
			        getStatsCalculator(StatsCategory.OUT, StatsNames.OUT, Components.AVERAGE.get(General.NAME)));

			ApplicationBean.getInstance().addAccumulator(groupByConfigBean.getName(), Components.AVERAGE.get(General.NAME), outAnvizentAccumulator);
		}

		AnvizentAccumulator errorAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), groupByConfigBean.getName(),
		        Components.AVERAGE.get(General.NAME), StatsCategory.ERROR, StatsNames.ERROR, true,
		        getStatsCalculator(StatsCategory.ERROR, StatsNames.ERROR, Components.AVERAGE.get(General.NAME)));

		ApplicationBean.getInstance().addAccumulator(groupByConfigBean.getName(), Components.AVERAGE.get(General.NAME), errorAnvizentAccumulator);
	}

	private void createAggregationAccumulators(GroupByConfigBean groupByConfigBean, StatsType statsType, boolean componentLevel) {
		if ((statsType.equals(StatsType.COMPONENT_LEVEL) && !componentLevel) || statsType.equals(StatsType.ALL)) {
			AnvizentAccumulator outAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), groupByConfigBean.getName(),
			        Components.AVERAGE.get(General.NAME), StatsCategory.OUT, StatsNames.OUT, !componentLevel,
			        getStatsCalculator(StatsCategory.OUT, StatsNames.OUT, Components.AGGREGATION.get(General.NAME)));

			ApplicationBean.getInstance().addAccumulator(groupByConfigBean.getName(), Components.AGGREGATION.get(General.NAME), outAnvizentAccumulator);
		}

		AnvizentAccumulator errorAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), groupByConfigBean.getName(),
		        Components.AVERAGE.get(General.NAME), StatsCategory.ERROR, StatsNames.ERROR, true,
		        getStatsCalculator(StatsCategory.ERROR, StatsNames.ERROR, Components.AGGREGATION.get(General.NAME)));

		ApplicationBean.getInstance().addAccumulator(groupByConfigBean.getName(), Components.AGGREGATION.get(General.NAME), errorAnvizentAccumulator);
	}

	@Override
	protected void createSpecialAccumulators(ConfigBean configBean, StatsType statsType, boolean componentLevel) {
		// TODO Auto-generated method stub
	}

	@Override
	public ResourceValidator getResourceConfigValidator() {
		// TODO Auto-generated method stub
		return null;
	}
}
