package com.anvizent.elt.core.spark.operation.factory;

import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.constant.Constants.StatsNames;
import com.anvizent.elt.core.lib.constant.StatsCategory;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.lib.stats.calculator.AnvizentMapToPairStatsCalculator;
import com.anvizent.elt.core.lib.stats.calculator.AnvizentStatsCalculator;
import com.anvizent.elt.core.lib.stats.calculator.IStatsCalculator;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.listener.common.constant.StatsType;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.common.util.ErrorHandlerUtil;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.Components;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.Join;
import com.anvizent.elt.core.spark.constant.Constants;
import com.anvizent.elt.core.spark.constant.JoinMode;
import com.anvizent.elt.core.spark.constant.JoinType;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.DuplicateColumnAliasException;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.operation.config.bean.JoinConfigBean;
import com.anvizent.elt.core.spark.operation.doc.helper.JoinDocHelper;
import com.anvizent.elt.core.spark.operation.function.BroadcastJoinFunction;
import com.anvizent.elt.core.spark.operation.function.FullOuterJoinFunction;
import com.anvizent.elt.core.spark.operation.function.LeftOuterJoinFunction;
import com.anvizent.elt.core.spark.operation.function.MapToPairFunction;
import com.anvizent.elt.core.spark.operation.function.RightOuterJoinFunction;
import com.anvizent.elt.core.spark.operation.function.SimpleJoinFunction;
import com.anvizent.elt.core.spark.operation.schema.validator.JoinSchemaValidator;
import com.anvizent.elt.core.spark.operation.validator.JoinValidator;
import com.anvizent.elt.core.spark.schema.validator.SchemaValidator;
import com.anvizent.elt.core.spark.util.CollectionUtil;
import com.anvizent.elt.core.spark.validator.ResourceValidator;
import com.anvizent.elt.core.spark.validator.Validator;

import scala.Tuple2;
import scala.reflect.ClassTag;

/**
 * @author Hareen Bejjanki
 *
 */
public class JoinFactory extends MultiInputOperationFactory {

	private static final long serialVersionUID = 1L;

	@Override
	protected Component process(ConfigBean configBean, LinkedHashMap<String, Component> components, ErrorHandlerSink errorHandlerSink) throws Exception {
		JoinConfigBean joinConfigBean = (JoinConfigBean) configBean;

		LinkedHashMap<String, String> lhsNewNames = new LinkedHashMap<>();
		LinkedHashMap<String, String> rhsNewNames = new LinkedHashMap<>();

		LinkedHashMap<String, AnvizentDataType> newStructure = getNewStructure(components.get(joinConfigBean.getSources().get(0)).getStructure(),
		        components.get(joinConfigBean.getSources().get(1)).getStructure(), joinConfigBean.getLHSPrefix(), joinConfigBean.getRHSPrefix(), lhsNewNames,
		        rhsNewNames);
		JavaRDD<HashMap<String, Object>> finalRDD;

		if (joinConfigBean.getJoinMode().equals(JoinMode.BROADCAST_RIGHT)) {
			finalRDD = rightBroadcastJoin(joinConfigBean, components, errorHandlerSink, lhsNewNames, rhsNewNames, newStructure);
		} else {

			JavaPairRDD<ArrayList<Object>, HashMap<String, Object>> pairedRDD1 = getPairedRDD(joinConfigBean, components, 0, Join.LEFT_HAND_SIDE,
			        errorHandlerSink);
			JavaPairRDD<ArrayList<Object>, HashMap<String, Object>> pairedRDD2 = getPairedRDD(joinConfigBean, components, 1, Join.RIGHT_HAND_SIDE,
			        errorHandlerSink);

			finalRDD = getFinalJoinedRDD(pairedRDD1, pairedRDD2, joinConfigBean, components.get(joinConfigBean.getSources().get(0)), newStructure, lhsNewNames,
			        rhsNewNames, errorHandlerSink);
		}

		return Component.createComponent(components.get(joinConfigBean.getSources().get(0)).getSparkSession(), joinConfigBean.getName(), finalRDD,
		        newStructure);
	}

	@SuppressWarnings("rawtypes")
	private JavaRDD<HashMap<String, Object>> rightBroadcastJoin(JoinConfigBean joinConfigBean, LinkedHashMap<String, Component> components,
	        ErrorHandlerSink errorHandlerSink, LinkedHashMap<String, String> lhsNewNames, LinkedHashMap<String, String> rhsNewNames,
	        LinkedHashMap<String, AnvizentDataType> newStructure)
	        throws UnimplementedException, ImproperValidationException, InvalidRelationException, UnsupportedException, Exception {
		Component leftComponent = components.get(joinConfigBean.getSources().get(0));

		JavaRDD<HashMap<String, Object>> leftRDD = leftComponent.getRDD(joinConfigBean.getSourceStream());
		JavaRDD<HashMap<String, Object>> rightRDD = components.get(joinConfigBean.getSources().get(1)).getRDD(joinConfigBean.getSourceStream());

		System.out.println(rightRDD.first());

		Broadcast<HashMap> broadcastData = ApplicationBean.getInstance().getSparkSession().sparkContext()
		        .broadcast(getRDDForBroadcast(rightRDD, joinConfigBean.getRHSFields()), classTag(HashMap.class));

		ArrayList<AnvizentAccumulator> accumulators = ApplicationBean.getInstance().getAccumulators(joinConfigBean.getName(),
		        joinConfigBean.getJoinType().name());
		AnvizentVoidFunction errorHandlerSinkFunction = ErrorHandlerUtil.getErrorHandlerFunction(joinConfigBean, leftComponent.getStructure(), errorHandlerSink,
		        joinConfigBean.getJoinType().name());
		JobDetails jobDetails = ErrorHandlerUtil.getJobDetails(joinConfigBean, joinConfigBean.getJoinType().name());

		return leftRDD.flatMap(new BroadcastJoinFunction(joinConfigBean, leftComponent.getStructure(), newStructure, joinConfigBean.getLHSFields(), lhsNewNames,
		        rhsNewNames, joinConfigBean.getJoinType(), broadcastData, accumulators, errorHandlerSinkFunction, jobDetails));
	}

	public static void main(String[] args) throws ParseException {
//		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS XXX");
//		SimpleDateFormat format = new SimpleDateFormat("");
//		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
//		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
		System.out.println(format.format(format.parse("2025-12-10 12:32:10:000 +01:00")));
		System.out.println(format.format(format.parse("2025-12-10 12:32:10:0000000 +01:00")));
	}

	private HashMap<LinkedList<Object>, HashMap<String, Object>> getRDDForBroadcast(JavaRDD<HashMap<String, Object>> rdd, ArrayList<String> joinKeys) {
		List<HashMap<String, Object>> data = rdd.collect();
		HashMap<LinkedList<Object>, HashMap<String, Object>> broadcastData = new HashMap<>();

		for (HashMap<String, Object> row : data) {
			LinkedList<Object> rowKey = getBroadcastRowKey(row, joinKeys);
			broadcastData.put(rowKey, row);
		}

		return broadcastData;
	}

	private LinkedList<Object> getBroadcastRowKey(HashMap<String, Object> row, ArrayList<String> joinKeys) {
		LinkedList<Object> rowKey = new LinkedList<>();

		for (String joinKey : joinKeys) {
			rowKey.add(row.get(joinKey));
		}

		return rowKey;
	}

	private static <T> ClassTag<T> classTag(Class<T> clazz) {
		return scala.reflect.ClassManifestFactory.fromClass(clazz);
	}

	private JavaPairRDD<ArrayList<Object>, HashMap<String, Object>> getPairedRDD(JoinConfigBean joinConfigBean, LinkedHashMap<String, Component> components,
	        int componentIndex, String configConstant, ErrorHandlerSink errorHandlerSink)
	        throws UnimplementedException, ImproperValidationException, InvalidRelationException, UnsupportedException, Exception {
		Component component = components.get(joinConfigBean.getSources().get(componentIndex));

		JavaPairRDD<ArrayList<Object>, HashMap<String, Object>> pairedRDD = component.getRDD(joinConfigBean.getSourceStream())
		        .mapToPair(new MapToPairFunction(joinConfigBean, component.getStructure(), component.getStructure(), configConstant,
		                ApplicationBean.getInstance().getAccumulators(joinConfigBean.getName(), configConstant),
		                ErrorHandlerUtil.getErrorHandlerFunction(joinConfigBean, component.getStructure(), errorHandlerSink, configConstant),
		                ErrorHandlerUtil.getJobDetails(joinConfigBean, configConstant)));

		if (joinConfigBean.isPersist()) {
			pairedRDD.persist(StorageLevel.MEMORY_AND_DISK());
		}

		return pairedRDD;
	}

	private JavaRDD<HashMap<String, Object>> getFinalJoinedRDD(JavaPairRDD<ArrayList<Object>, HashMap<String, Object>> pairedRDD1,
	        JavaPairRDD<ArrayList<Object>, HashMap<String, Object>> pairedRDD2, JoinConfigBean joinConfigBean, Component component,
	        LinkedHashMap<String, AnvizentDataType> newStructure, HashMap<String, String> lhsNewNames, HashMap<String, String> rhsNewNames,
	        ErrorHandlerSink errorHandlerSink)
	        throws UnimplementedException, ImproperValidationException, InvalidRelationException, UnsupportedException, Exception {
		ArrayList<AnvizentAccumulator> accumulators = ApplicationBean.getInstance().getAccumulators(joinConfigBean.getName(),
		        joinConfigBean.getJoinType().name());

		AnvizentVoidFunction errorHandlerSinkFunction = ErrorHandlerUtil.getErrorHandlerFunction(joinConfigBean, component.getStructure(), errorHandlerSink,
		        joinConfigBean.getJoinType().name());

		JobDetails jobDetails = ErrorHandlerUtil.getJobDetails(joinConfigBean, joinConfigBean.getJoinType().name());

		if (joinConfigBean.getJoinType().equals(JoinType.SIMPLE_JOIN)) {
			JavaPairRDD<ArrayList<Object>, Tuple2<HashMap<String, Object>, HashMap<String, Object>>> joinRDD = pairedRDD1.join(pairedRDD2);
			return joinRDD.values().flatMap(new SimpleJoinFunction(joinConfigBean, component.getStructure(), newStructure, lhsNewNames, rhsNewNames,
			        accumulators, errorHandlerSinkFunction, jobDetails));
		} else if (joinConfigBean.getJoinType().equals(JoinType.LEFT_OUTER_JOIN)) {
			JavaPairRDD<ArrayList<Object>, Tuple2<HashMap<String, Object>, Optional<HashMap<String, Object>>>> joinRDD = pairedRDD1.leftOuterJoin(pairedRDD2);
			return joinRDD.values().flatMap(new LeftOuterJoinFunction(joinConfigBean, component.getStructure(), newStructure, lhsNewNames, rhsNewNames,
			        accumulators, errorHandlerSinkFunction, jobDetails));
		} else if (joinConfigBean.getJoinType().equals(JoinType.RIGHT_OUTER_JOIN)) {
			JavaPairRDD<ArrayList<Object>, Tuple2<Optional<HashMap<String, Object>>, HashMap<String, Object>>> joinRDD = pairedRDD1.rightOuterJoin(pairedRDD2);
			return joinRDD.values().flatMap(new RightOuterJoinFunction(joinConfigBean, component.getStructure(), newStructure, lhsNewNames, rhsNewNames,
			        accumulators, errorHandlerSinkFunction, jobDetails));
		} else {
			JavaPairRDD<ArrayList<Object>, Tuple2<Optional<HashMap<String, Object>>, Optional<HashMap<String, Object>>>> joinRDD = pairedRDD1
			        .fullOuterJoin(pairedRDD2);
			return joinRDD.values().flatMap(new FullOuterJoinFunction(joinConfigBean, component.getStructure(), newStructure, lhsNewNames, rhsNewNames,
			        accumulators, errorHandlerSinkFunction, jobDetails));
		}
	}

	private LinkedHashMap<String, AnvizentDataType> getNewStructure(LinkedHashMap<String, AnvizentDataType> lhsStructure,
	        LinkedHashMap<String, AnvizentDataType> rhsStructure, String lhsPrefix, String rhsPrefix, LinkedHashMap<String, String> lhsNewNames,
	        LinkedHashMap<String, String> rhsNewNames) throws DuplicateColumnAliasException {
		LinkedHashMap<String, AnvizentDataType> newStructure = new LinkedHashMap<>();

		putNewStructure(newStructure, lhsStructure, rhsStructure, lhsPrefix, rhsPrefix, lhsNewNames);
		putNewStructure(newStructure, rhsStructure, lhsStructure, rhsPrefix, lhsPrefix, rhsNewNames);

		return newStructure;
	}

	private void putNewStructure(LinkedHashMap<String, AnvizentDataType> newStructure, LinkedHashMap<String, AnvizentDataType> structure,
	        LinkedHashMap<String, AnvizentDataType> optionalStructure, String prefix, String optionalPrefix, LinkedHashMap<String, String> newNames)
	        throws DuplicateColumnAliasException {

		for (Entry<String, AnvizentDataType> entry : structure.entrySet()) {
			String key = entry.getKey();
			AnvizentDataType value = entry.getValue();

			if (optionalStructure.get(key) != null) {
				if (!prefix.isEmpty()) {
					CollectionUtil.putWithPrefix(newStructure, prefix, key, value);
					CollectionUtil.putWithPrefix(newNames, prefix, key, key);
				} else if (prefix.isEmpty() && optionalPrefix.isEmpty()) {
					throw new DuplicateColumnAliasException(
					        MessageFormat.format(Constants.ExceptionMessage.DUPLICATE_COLUMN_ALIASES_FOUND_IN_LHS_SOURCE_AND_RHS_SOURCE, key));
				} else {
					newStructure.put(key, value);
					newNames.put(key, key);
				}
			} else {
				newStructure.put(key, value);
				newNames.put(key, key);
			}
		}
	}

	@Override
	public String getName() {
		return Components.JOIN.get(General.NAME);
	}

	@Override
	public DocHelper getDocHelper() throws InvalidParameter {
		return new JoinDocHelper(this);
	}

	@Override
	public Validator getValidator() {
		return new JoinValidator(this);
	}

	@Override
	public SchemaValidator getSchemaValidator() {
		return new JoinSchemaValidator();
	}

	@Override
	public Integer getMaxInputs() {
		return 2;
	}

	@Override
	public Integer getMinInputs() {
		return 2;
	}

	@Override
	protected IStatsCalculator getStatsCalculator(StatsCategory statsCategory, String statsName, String internalRDDName) {
		if (internalRDDName.equals(Join.LEFT_HAND_SIDE) || internalRDDName.equals(Join.RIGHT_HAND_SIDE)) {
			return new AnvizentMapToPairStatsCalculator<LinkedHashMap<String, Object>, ArrayList<Object>>(statsCategory, statsName);
		} else if (internalRDDName.equals(JoinType.SIMPLE_JOIN.name())) {
			return new AnvizentStatsCalculator<Tuple2<LinkedHashMap<String, Object>, LinkedHashMap<String, Object>>, LinkedHashMap<String, Object>>(
			        statsCategory, statsName);
		} else if (internalRDDName.equals(JoinType.LEFT_OUTER_JOIN.name())) {
			return new AnvizentStatsCalculator<Tuple2<LinkedHashMap<String, Object>, Optional<LinkedHashMap<String, Object>>>, LinkedHashMap<String, Object>>(
			        statsCategory, statsName);
		} else if (internalRDDName.equals(JoinType.RIGHT_OUTER_JOIN.name())) {
			return new AnvizentStatsCalculator<Tuple2<Optional<LinkedHashMap<String, Object>>, LinkedHashMap<String, Object>>, LinkedHashMap<String, Object>>(
			        statsCategory, statsName);
		} else {
			return new AnvizentStatsCalculator<Tuple2<Optional<LinkedHashMap<String, Object>>, Optional<LinkedHashMap<String, Object>>>, LinkedHashMap<String, Object>>(
			        statsCategory, statsName);
		}
	}

	@Override
	public void createAccumulators(ConfigAndMappingConfigBeans configAndMappingConfigBeans, StatsType globalStatsType) {
		StatsType statsType = getStatsType(configAndMappingConfigBeans.getStatsStore(), globalStatsType);

		if (statsType.equals(StatsType.NONE)) {
			return;
		}

		boolean componentLevel = getNotEmptyMappingConfigBeans(configAndMappingConfigBeans.getMappingConfigBeans()) > 0 ? true : false;
		createSpecialAccumulators(configAndMappingConfigBeans.getConfigBean(), statsType, componentLevel);
		createFactoryAccumulators(configAndMappingConfigBeans.getConfigBean(), statsType, componentLevel);
	}

	@Override
	protected void createFactoryAccumulators(ConfigBean configBean, StatsType statsType, boolean componentLevel) {
		createJoinRDDAccumulators(configBean, statsType, componentLevel);
	}

	private void createLHSRDDAccumulators(ConfigBean configBean, StatsType statsType, boolean componentLevel) {
		AnvizentAccumulator lhsInAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
		        Join.LEFT_HAND_SIDE, StatsCategory.IN, StatsNames.IN, true, getStatsCalculator(StatsCategory.IN, StatsNames.IN, Join.LEFT_HAND_SIDE));
		AnvizentAccumulator lhsErrorInAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
		        Join.LEFT_HAND_SIDE, StatsCategory.ERROR, StatsNames.ERROR, true,
		        getStatsCalculator(StatsCategory.ERROR, StatsNames.ERROR, Join.LEFT_HAND_SIDE));

		ApplicationBean.getInstance().addAccumulator(configBean.getName(), Join.LEFT_HAND_SIDE, lhsErrorInAnvizentAccumulator, lhsInAnvizentAccumulator);

		if (statsType.equals(StatsType.ALL)) {
			AnvizentAccumulator lhsOutAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
			        Join.LEFT_HAND_SIDE, StatsCategory.OUT, StatsNames.OUT, false, getStatsCalculator(StatsCategory.OUT, StatsNames.OUT, Join.LEFT_HAND_SIDE));

			ApplicationBean.getInstance().addAccumulator(configBean.getName(), Join.LEFT_HAND_SIDE, lhsOutAnvizentAccumulator);
		}
	}

	private void createRHSRDDAccumulators(ConfigBean configBean, StatsType statsType, boolean componentLevel) {
		AnvizentAccumulator rhsInAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
		        Join.RIGHT_HAND_SIDE, StatsCategory.IN, StatsNames.IN, true, getStatsCalculator(StatsCategory.IN, StatsNames.IN, Join.RIGHT_HAND_SIDE));
		AnvizentAccumulator rhsErrorInAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
		        Join.RIGHT_HAND_SIDE, StatsCategory.ERROR, StatsNames.ERROR, true,
		        getStatsCalculator(StatsCategory.ERROR, StatsNames.ERROR, Join.RIGHT_HAND_SIDE));

		ApplicationBean.getInstance().addAccumulator(configBean.getName(), Join.RIGHT_HAND_SIDE, rhsErrorInAnvizentAccumulator, rhsInAnvizentAccumulator);

		if (statsType.equals(StatsType.ALL)) {
			AnvizentAccumulator rhsOutAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
			        Join.RIGHT_HAND_SIDE, StatsCategory.OUT, StatsNames.OUT, false,
			        getStatsCalculator(StatsCategory.OUT, StatsNames.OUT, Join.RIGHT_HAND_SIDE));

			ApplicationBean.getInstance().addAccumulator(configBean.getName(), Join.RIGHT_HAND_SIDE, rhsOutAnvizentAccumulator);
		}
	}

	private void createJoinRDDAccumulators(ConfigBean configBean, StatsType statsType, boolean componentLevel) {
		String internalRDDName = getInternalRDDName((JoinConfigBean) configBean);

		AnvizentAccumulator joinErrorAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
		        internalRDDName, StatsCategory.ERROR, StatsNames.ERROR, true, getStatsCalculator(StatsCategory.ERROR, StatsNames.ERROR, internalRDDName));
		ApplicationBean.getInstance().addAccumulator(configBean.getName(), internalRDDName, joinErrorAnvizentAccumulator);

		if (statsType.equals(StatsType.ALL)) {
			AnvizentAccumulator joinInAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
			        internalRDDName, StatsCategory.IN, StatsNames.IN, false, getStatsCalculator(StatsCategory.IN, StatsNames.IN, internalRDDName));
			ApplicationBean.getInstance().addAccumulator(configBean.getName(), internalRDDName, joinInAnvizentAccumulator);
		}

		if ((statsType.equals(StatsType.COMPONENT_LEVEL) && !componentLevel) || statsType.equals(StatsType.ALL)) {
			AnvizentAccumulator joinOutAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
			        internalRDDName, StatsCategory.OUT, StatsNames.OUT, !componentLevel,
			        getStatsCalculator(StatsCategory.OUT, StatsNames.OUT, internalRDDName));

			ApplicationBean.getInstance().addAccumulator(configBean.getName(), internalRDDName, joinOutAnvizentAccumulator);
		}
	}

	private String getInternalRDDName(JoinConfigBean joinConfigBean) {
		if (joinConfigBean.getJoinType().equals(JoinType.SIMPLE_JOIN)) {
			return JoinType.SIMPLE_JOIN.name();
		} else if (joinConfigBean.getJoinType().equals(JoinType.LEFT_OUTER_JOIN)) {
			return JoinType.LEFT_OUTER_JOIN.name();
		} else if (joinConfigBean.getJoinType().equals(JoinType.RIGHT_OUTER_JOIN)) {
			return JoinType.RIGHT_OUTER_JOIN.name();
		} else {
			return JoinType.FULL_OUTER_JOIN.name();
		}
	}

	@Override
	protected void createSpecialAccumulators(ConfigBean configBean, StatsType statsType, boolean componentLevel) {
		createLHSRDDAccumulators(configBean, statsType, componentLevel);
		createRHSRDDAccumulators(configBean, statsType, componentLevel);
	}

	@Override
	public ResourceValidator getResourceConfigValidator() {
		// TODO Auto-generated method stub
		return null;
	}
}
