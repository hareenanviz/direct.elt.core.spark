package com.anvizent.elt.core.spark.operation.factory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.spark.api.java.JavaRDD;

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
import com.anvizent.elt.core.lib.function.EmptyFunction;
import com.anvizent.elt.core.lib.stats.calculator.AnvizentStatsCalculator;
import com.anvizent.elt.core.lib.stats.calculator.IStatsCalculator;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.listener.common.constant.StatsType;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.common.util.ErrorHandlerUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.Components;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.Union;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.exception.InvalidUnionException;
import com.anvizent.elt.core.spark.operation.config.bean.UnionConfigBean;
import com.anvizent.elt.core.spark.operation.doc.helper.UnionDocHelper;
import com.anvizent.elt.core.spark.operation.function.UnionFunction;
import com.anvizent.elt.core.spark.operation.validator.UnionValidator;
import com.anvizent.elt.core.spark.schema.validator.SchemaValidator;
import com.anvizent.elt.core.spark.validator.ResourceValidator;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public class UnionFactory extends MultiInputOperationFactory {

	private static final long serialVersionUID = 1L;

	@Override
	protected Component process(ConfigBean configBean, LinkedHashMap<String, Component> components, ErrorHandlerSink errorHandlerSink) throws Exception {
		UnionConfigBean unionConfigBean = (UnionConfigBean) configBean;

		LinkedHashMap<String, AnvizentDataType> newStructure = getNewStructure(unionConfigBean, components);

		Component baseComponent = components.get(unionConfigBean.getSources().get(0));

		JavaRDD<HashMap<String, Object>> javaRDD_0 = get0thRDD(unionConfigBean, components);
		JavaRDD<HashMap<String, Object>> unionJavaRDD = getUnionedRDD(unionConfigBean, components, javaRDD_0);
		JavaRDD<HashMap<String, Object>> unionFunctionJavaRDD = getUnionFunctionRDD(unionConfigBean, baseComponent, newStructure, unionJavaRDD,
		        errorHandlerSink);

		return Component.createComponent(baseComponent.getSparkSession(), unionConfigBean.getName(), unionFunctionJavaRDD, newStructure);
	}

	private JavaRDD<HashMap<String, Object>> get0thRDD(UnionConfigBean unionConfigBean, LinkedHashMap<String, Component> components) {
		return components.get(unionConfigBean.getSources().get(0)).getRDD();
	}

	private JavaRDD<HashMap<String, Object>> getUnionedRDD(UnionConfigBean unionConfigBean, LinkedHashMap<String, Component> components,
	        JavaRDD<HashMap<String, Object>> unionJavaRDD) {
		for (int i = 1; i < unionConfigBean.getSources().size(); i++) {
			unionJavaRDD = unionJavaRDD.union(components.get(unionConfigBean.getSources().get(i)).getRDD());
		}

		return unionJavaRDD;
	}

	private JavaRDD<HashMap<String, Object>> getUnionFunctionRDD(UnionConfigBean unionConfigBean, Component baseComponent,
	        LinkedHashMap<String, AnvizentDataType> newStructure, JavaRDD<HashMap<String, Object>> unionFunctionJavaRDD, ErrorHandlerSink errorHandlerSink)
	        throws UnimplementedException, ImproperValidationException, InvalidRelationException, UnsupportedException, Exception {

		ArrayList<AnvizentAccumulator> accumulators = ApplicationBean.getInstance().getAccumulators(unionConfigBean.getName(), getName());
		AnvizentVoidFunction errorHandlerSinkFunction = ErrorHandlerUtil.getErrorHandlerFunction(unionConfigBean, baseComponent.getStructure(),
		        errorHandlerSink, getName());
		JobDetails jobDetails = ErrorHandlerUtil.getJobDetails(unionConfigBean, getName());

		if (unionConfigBean.getStructureSourceName() != null && !unionConfigBean.getStructureSourceName().isEmpty()) {
			unionFunctionJavaRDD = unionFunctionJavaRDD.flatMap(
			        new UnionFunction(unionConfigBean, baseComponent.getStructure(), newStructure, accumulators, errorHandlerSinkFunction, jobDetails));
		} else if (ApplicationBean.getInstance().getStatsSettingsStore() != null) {
			unionFunctionJavaRDD = unionFunctionJavaRDD.flatMap(
			        new EmptyFunction(unionConfigBean, baseComponent.getStructure(), newStructure, accumulators, errorHandlerSinkFunction, jobDetails));
		}

		return unionFunctionJavaRDD;
	}

	private LinkedHashMap<String, AnvizentDataType> getNewStructure(UnionConfigBean unionConfigBean, LinkedHashMap<String, Component> components)
	        throws InvalidUnionException {
		LinkedHashMap<String, AnvizentDataType> newStructure = new LinkedHashMap<>();
		ArrayList<String> componentNames = unionConfigBean.getSources();

		if (unionConfigBean.getStructureSourceName() == null || unionConfigBean.getStructureSourceName().isEmpty()) {
			compareAllStructures(components, componentNames, newStructure);
		} else {
			compareWithSourceStructure(unionConfigBean, components, componentNames, newStructure);
		}

		return newStructure;
	}

	private void compareAllStructures(LinkedHashMap<String, Component> components, ArrayList<String> componentNames,
	        LinkedHashMap<String, AnvizentDataType> newStructure) throws InvalidUnionException {
		validateComponent(0, components, componentNames);
		LinkedHashMap<String, AnvizentDataType> structure_0 = components.get(componentNames.get(0)).getStructure();
		newStructure.putAll(structure_0);

		for (int i = 1; i < componentNames.size(); i++) {
			validateComponent(i, components, componentNames);
			LinkedHashMap<String, AnvizentDataType> structure_i = components.get(componentNames.get(i)).getStructure();
			compareStructures(structure_0, componentNames.get(0), structure_i, componentNames.get(i), true);
		}
	}

	private void validateComponent(int index, LinkedHashMap<String, Component> components, ArrayList<String> componentNames) throws InvalidUnionException {
		if (!components.containsKey(componentNames.get(index))) {
			throw new InvalidUnionException("No source component found with component name '" + componentNames.get(0) + "'");
		}
	}

	private void compareWithSourceStructure(UnionConfigBean unionConfigBean, LinkedHashMap<String, Component> components, ArrayList<String> keySet,
	        LinkedHashMap<String, AnvizentDataType> newStructure) throws InvalidUnionException {
		if (!components.containsKey(unionConfigBean.getStructureSourceName())) {
			throw new InvalidUnionException(
			        Union.STRUCTURE_SOURCE_NAME + ": " + unionConfigBean.getStructureSourceName() + " is not present in source components!");
		}

		LinkedHashMap<String, AnvizentDataType> sourceStructure = components.get(unionConfigBean.getStructureSourceName()).getStructure();
		newStructure.putAll(sourceStructure);
		keySet.remove(unionConfigBean.getStructureSourceName());

		for (int i = 0; i < keySet.size(); i++) {
			validateComponent(i, components, keySet);
			LinkedHashMap<String, AnvizentDataType> structure_i = components.get(keySet.get(i)).getStructure();
			compareStructures(sourceStructure, unionConfigBean.getStructureSourceName(), structure_i, keySet.get(i), false);
		}
	}

	private void compareStructures(LinkedHashMap<String, AnvizentDataType> structure_0, String source_0, LinkedHashMap<String, AnvizentDataType> structure_i,
	        String source_i, boolean checkFields) throws InvalidUnionException {
		if (structure_0.size() != structure_i.size()) {
			throw new InvalidUnionException("Structure mismatch for " + source_0 + " and " + source_i);
		}

		ArrayList<String> keySet_0 = new ArrayList<>(structure_0.keySet());
		ArrayList<String> keySet_i = new ArrayList<>(structure_i.keySet());

		compareStructures(structure_0, keySet_0, source_0, structure_i, keySet_i, source_i, checkFields);
	}

	private void compareStructures(LinkedHashMap<String, AnvizentDataType> structure_0, ArrayList<String> keySet_0, String source_0,
	        LinkedHashMap<String, AnvizentDataType> structure_i, ArrayList<String> keySet_i, String source_i, boolean checkFields)
	        throws InvalidUnionException {
		for (int i = 0; i < keySet_0.size(); i++) {
			String key_0 = keySet_0.get(i);
			String key_i = keySet_i.get(i);

			if (checkFields && !key_0.equals(key_i)) {
				throw new InvalidUnionException("Structure mismatch for " + source_0 + " and " + source_i);
			}

			if (!structure_0.get(key_0).equals(structure_i.get(key_i))) {
				throw new InvalidUnionException("Structure mismatch for " + source_0 + " and " + source_i);
			}
		}
	}

	@Override
	public String getName() {
		return Components.UNION.get(General.NAME);
	}

	@Override
	public DocHelper getDocHelper() throws InvalidParameter {
		return new UnionDocHelper(this);
	}

	@Override
	public Validator getValidator() {
		return new UnionValidator(this);
	}

	@Override
	public SchemaValidator getSchemaValidator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Integer getMaxInputs() {
		return null;
	}

	@Override
	public Integer getMinInputs() {
		return 2;
	}

	@Override
	protected IStatsCalculator getStatsCalculator(StatsCategory statsCategory, String statsName, String internalRDDName) {
		return new AnvizentStatsCalculator<LinkedHashMap<String, Object>, LinkedHashMap<String, Object>>(statsCategory, statsName);
	}

	@Override
	protected void createFactoryAccumulators(ConfigBean configBean, StatsType statsType, boolean componentLevel) {
		AnvizentAccumulator inAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(), getName(),
		        StatsCategory.IN, StatsNames.IN, true, getStatsCalculator(StatsCategory.IN, StatsNames.IN, getName()));

		ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName(), inAnvizentAccumulator);

		if ((statsType.equals(StatsType.COMPONENT_LEVEL) && !componentLevel) || statsType.equals(StatsType.ALL)) {
			AnvizentAccumulator outAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), configBean.getName(),
			        getName(), StatsCategory.OUT, StatsNames.OUT, !componentLevel, getStatsCalculator(StatsCategory.OUT, StatsNames.OUT, getName()));

			ApplicationBean.getInstance().addAccumulator(configBean.getName(), getName(), outAnvizentAccumulator);
		}
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
