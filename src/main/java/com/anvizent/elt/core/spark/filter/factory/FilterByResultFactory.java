package com.anvizent.elt.core.spark.filter.factory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.constant.Constants.StatsNames;
import com.anvizent.elt.core.lib.constant.StatsCategory;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.lib.stats.calculator.AnvizentFilterStatsCalculator;
import com.anvizent.elt.core.lib.stats.calculator.IStatsCalculator;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.listener.common.constant.StatsType;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.common.util.ErrorHandlerUtil;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Filter.Components;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.filter.config.bean.FilterByResultConfigBean;
import com.anvizent.elt.core.spark.filter.doc.helper.FilterByResultDocHelper;
import com.anvizent.elt.core.spark.filter.function.FilterByResultFunction;
import com.anvizent.elt.core.spark.filter.validator.FilterByResultValidator;
import com.anvizent.elt.core.spark.operation.config.bean.ResultFetcherConfigBean;
import com.anvizent.elt.core.spark.operation.factory.SimpleOperationFactory;
import com.anvizent.elt.core.spark.schema.validator.SchemaValidator;
import com.anvizent.elt.core.spark.validator.ResourceValidator;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class FilterByResultFactory extends SimpleOperationFactory {

	private static final long serialVersionUID = 1L;

	@Override
	protected Component process(ConfigBean configBean, Component component, ErrorHandlerSink errorHandlerSink) throws Exception {
		FilterByResultConfigBean filterByResultConfigBean = (FilterByResultConfigBean) configBean;

		verifyAndDecodeIfElseMethods(filterByResultConfigBean, component.getStructure());

		LinkedHashMap<String, AnvizentDataType> newStructure = new LinkedHashMap<>(component.getStructure());

		JavaRDD<HashMap<String, Object>> filteredRDD = getFilteredRDD(0, filterByResultConfigBean, component, newStructure,
		        getAccumulators(0, filterByResultConfigBean), getErrorHandlerSinkFunction(0, filterByResultConfigBean, component, errorHandlerSink),
		        getJobDetails(0, filterByResultConfigBean));

		Component finalComponent = Component.createComponent(ApplicationBean.getInstance().getSparkSession(), filterByResultConfigBean.getName(),
		        filterByResultConfigBean.getEmitStreamNames().get(0), filteredRDD, newStructure);

		for (int i = 1; i < filterByResultConfigBean.getEmitStreamNames().size(); i++) {
			finalComponent.addRDD(filterByResultConfigBean.getEmitStreamNames().get(i),
			        getFilteredRDD(i, filterByResultConfigBean, component, newStructure, getAccumulators(i, filterByResultConfigBean),
			                getErrorHandlerSinkFunction(i, filterByResultConfigBean, component, errorHandlerSink), getJobDetails(i, filterByResultConfigBean)));
		}

		return finalComponent;
	}

	private JavaRDD<HashMap<String, Object>> getFilteredRDD(int index, FilterByResultConfigBean filterByResultConfigBean, Component component,
	        LinkedHashMap<String, AnvizentDataType> newStructure, ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction errorHandlerSinkFunction,
	        JobDetails jobDetails) throws InvalidArgumentsException {
		JavaRDD<HashMap<String, Object>> filteredRDD = component.getRDD(filterByResultConfigBean.getSourceStream()).filter(new FilterByResultFunction(
		        filterByResultConfigBean, index, component.getStructure(), newStructure, accumulators, errorHandlerSinkFunction, jobDetails));

		if (filterByResultConfigBean.isPersist()) {
			filteredRDD = filteredRDD.persist(StorageLevel.MEMORY_AND_DISK());
		}

		return filteredRDD;
	}

	private ArrayList<AnvizentAccumulator> getAccumulators(int index, FilterByResultConfigBean filterByResultConfigBean) {
		ArrayList<AnvizentAccumulator> accumulators = new ArrayList<>();

		if (ApplicationBean.getInstance().getAccumulators(filterByResultConfigBean.getName(), getName()) != null) {
			accumulators.addAll(ApplicationBean.getInstance().getAccumulators(filterByResultConfigBean.getName(), getName()));
		}

		if (ApplicationBean.getInstance().getAccumulators(filterByResultConfigBean.getName(),
		        getName() + "_" + filterByResultConfigBean.getEmitStreamNames().get(index)) != null) {
			accumulators.addAll(ApplicationBean.getInstance().getAccumulators(filterByResultConfigBean.getName(),
			        getName() + "_" + filterByResultConfigBean.getEmitStreamNames().get(index)));
		}

		return accumulators;
	}

	private AnvizentVoidFunction getErrorHandlerSinkFunction(int index, FilterByResultConfigBean filterByResultConfigBean, Component component,
	        ErrorHandlerSink errorHandlerSink)
	        throws UnimplementedException, ImproperValidationException, InvalidRelationException, UnsupportedException, Exception {
		AnvizentVoidFunction errorHandlerSinkFunction = ErrorHandlerUtil.getErrorHandlerFunction(filterByResultConfigBean, component.getStructure(),
		        errorHandlerSink, getName() + "_" + filterByResultConfigBean.getEmitStreamNames().get(index));

		return errorHandlerSinkFunction;
	}

	private JobDetails getJobDetails(int index, FilterByResultConfigBean filterByResultConfigBean)
	        throws UnimplementedException, ImproperValidationException, InvalidRelationException, UnsupportedException, Exception {
		JobDetails jobDetails = ErrorHandlerUtil.getJobDetails(filterByResultConfigBean,
		        getName() + "_" + filterByResultConfigBean.getEmitStreamNames().get(index));

		return jobDetails;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void verifyAndDecodeIfElseMethods(FilterByResultConfigBean filterByResultConfigBean, LinkedHashMap<String, AnvizentDataType> structure)
	        throws InvalidConfigException {
		InvalidConfigException invalidConfigException = generateException(filterByResultConfigBean);

		try {
			int previousIndex = -1;
			for (int i = 0; i < filterByResultConfigBean.getMethodNames().size(); i++) {
				ArrayList<String> decodedClassNames = new ArrayList<>();
				ArrayList<String> decodedMethods = new ArrayList<>();
				ArrayList<String> decodedMethodTypes = new ArrayList<>();
				ArrayList<ArrayList<String>> decodedMethodArguments = new ArrayList<>();
				ArrayList<Class[]> decodedMethodArgumentTypes = new ArrayList<>();
				ArrayList<String> decodedMethodResultTypes = new ArrayList<>();

				Class className = Class.forName(filterByResultConfigBean.getClassNames().get(i));
				Class[] methodArgumentTypes = getMethodArguments(filterByResultConfigBean.getMethodArgumentFields().get(i), structure, invalidConfigException);
				Method method = className.getMethod(filterByResultConfigBean.getMethodNames().get(i), methodArgumentTypes);

				if (!method.getReturnType().equals(Boolean.TYPE)) {
					invalidConfigException.add("Method : '" + filterByResultConfigBean.getMethodNames().get(i) + "' does not returns a boolean.");
					throw invalidConfigException;
				} else {
					if (i > 0) {
						addPreviousMethods(++previousIndex, filterByResultConfigBean, decodedClassNames, decodedMethods, decodedMethodTypes,
						        decodedMethodArguments, decodedMethodArgumentTypes, decodedMethodResultTypes);
					}

					decodedMethodTypes.add(Modifier.isStatic(method.getModifiers()) ? "static" : "non-static");
					decodedClassNames.add(filterByResultConfigBean.getClassNames().get(i));
					decodedMethods.add(filterByResultConfigBean.getMethodNames().get(i));
					decodedMethodArguments.add(filterByResultConfigBean.getMethodArgumentFields().get(i));
					decodedMethodArgumentTypes.add(methodArgumentTypes);
					decodedMethodResultTypes.add("LIKE");

					if (Modifier.isStatic(method.getModifiers())) {
						filterByResultConfigBean.putClass(filterByResultConfigBean.getClassNames().get(i), className);
					} else {
						filterByResultConfigBean.putClassObject(filterByResultConfigBean.getClassNames().get(i), getClassObject(className));
					}

					addToConfigBean(filterByResultConfigBean, decodedClassNames, decodedMethods, decodedMethodTypes, decodedMethodArguments,
					        decodedMethodArgumentTypes, decodedMethodResultTypes);
				}
			}

			if (filterByResultConfigBean.getEmitStreamNames().size() > filterByResultConfigBean.getMethodNames().size()) {
				ArrayList<String> decodedClassNames = new ArrayList<>();
				ArrayList<String> decodedMethods = new ArrayList<>();
				ArrayList<String> decodedMethodTypes = new ArrayList<>();
				ArrayList<ArrayList<String>> decodedMethodArguments = new ArrayList<>();
				ArrayList<Class[]> decodedMethodArgumentTypes = new ArrayList<>();
				ArrayList<String> decodedMethodResultTypes = new ArrayList<>();

				addPreviousMethods(++previousIndex, filterByResultConfigBean, decodedClassNames, decodedMethods, decodedMethodTypes, decodedMethodArguments,
				        decodedMethodArgumentTypes, decodedMethodResultTypes);

				addToConfigBean(filterByResultConfigBean, decodedClassNames, decodedMethods, decodedMethodTypes, decodedMethodArguments,
				        decodedMethodArgumentTypes, decodedMethodResultTypes);
			}

		} catch (ClassNotFoundException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException
		        | InvocationTargetException exception) {
			invalidConfigException.add(exception.getMessage(), exception);
		}

		if (invalidConfigException.getNumberOfExceptions() > 0) {
			throw invalidConfigException;
		}
	}

	@SuppressWarnings("rawtypes")
	private void addToConfigBean(FilterByResultConfigBean filterByResultConfigBean, ArrayList<String> decodedClassNames, ArrayList<String> decodedMethods,
	        ArrayList<String> decodedMethodTypes, ArrayList<ArrayList<String>> decodedMethodArguments, ArrayList<Class[]> decodedMethodArgumentTypes,
	        ArrayList<String> decodedMethodResultTypes) {
		filterByResultConfigBean.addDecodedClassNames(decodedClassNames);
		filterByResultConfigBean.addDecodedMethods(decodedMethods);
		filterByResultConfigBean.addDecodedMethodTypes(decodedMethodTypes);
		filterByResultConfigBean.addDecodedMethodArguments(decodedMethodArguments);
		filterByResultConfigBean.addDecodedMethodArgumentTypes(decodedMethodArgumentTypes);
		filterByResultConfigBean.addDecodedMethodResultTypes(decodedMethodResultTypes);
	}

	@SuppressWarnings("rawtypes")
	private void addPreviousMethods(int previousIndex, FilterByResultConfigBean filterByResultConfigBean, ArrayList<String> decodedClassNames,
	        ArrayList<String> decodedMethods, ArrayList<String> decodedMethodTypes, ArrayList<ArrayList<String>> decodedMethodArguments,
	        ArrayList<Class[]> decodedMethodArgumentTypes, ArrayList<String> decodedMethodResultTypes) {
		decodedClassNames.addAll(filterByResultConfigBean.getDecodedClassNames().get(previousIndex));
		decodedMethods.addAll(filterByResultConfigBean.getDecodedMethods().get(previousIndex));
		decodedMethodTypes.addAll(filterByResultConfigBean.getDecodedMethodTypes().get(previousIndex));
		decodedMethodArguments.addAll(filterByResultConfigBean.getDecodedMethodArguments().get(previousIndex));
		decodedMethodArgumentTypes.addAll(filterByResultConfigBean.getDecodedMethodArgumentTypes().get(previousIndex));
		decodedMethodResultTypes.addAll(generateMethodResultTypes(filterByResultConfigBean.getDecodedMethods().get(previousIndex).size()));
	}

	private ArrayList<String> generateMethodResultTypes(int size) {
		String str = "NOT_LIKE";
		str = StringUtils.repeat(str, ",", size);

		return new ArrayList<>(Arrays.asList(str.split(",")));
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Object getClassObject(Class className) throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException,
	        IllegalArgumentException, InvocationTargetException {
		Constructor<?> constructor = className.getConstructor(new Class[] {});
		Object classObject = constructor.newInstance(new Object[] {});

		return classObject;
	}

	private InvalidConfigException generateException(ResultFetcherConfigBean resultFetcherConfigBean) {
		InvalidConfigException invalidConfigException = new InvalidConfigException();

		invalidConfigException.setSeekDetails(resultFetcherConfigBean.getSeekDetails());
		invalidConfigException.setComponent(resultFetcherConfigBean.getConfigName());
		invalidConfigException.setComponentName(resultFetcherConfigBean.getName());

		return invalidConfigException;
	}

	@SuppressWarnings("rawtypes")
	private Class[] getMethodArguments(ArrayList<String> methodArgumentFields, LinkedHashMap<String, AnvizentDataType> structure,
	        InvalidConfigException invalidConfigException) {
		if (methodArgumentFields == null || methodArgumentFields.isEmpty()) {
			return new Class[] {};
		} else {
			Class[] methodArgumentTypes = new Class[methodArgumentFields.size()];

			for (int i = 0; i < methodArgumentFields.size(); i++) {
				if (methodArgumentFields.get(i) != null && !methodArgumentFields.get(i).isEmpty() && structure.get(methodArgumentFields.get(i)) == null) {
					invalidConfigException.add("'" + methodArgumentFields.get(i) + "' field not found in source structure.");
				} else {
					methodArgumentTypes[i] = structure.get(methodArgumentFields.get(i)).getJavaType();
				}
			}

			return methodArgumentTypes;
		}
	}

	@Override
	public String getName() {
		return Components.FILTER_BY_RESULT.get(General.NAME);
	}

	@Override
	public DocHelper getDocHelper() throws InvalidParameter {
		return new FilterByResultDocHelper(this);
	}

	@Override
	public Validator getValidator() {
		return new FilterByResultValidator(this);
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
	public void createAccumulators(ConfigAndMappingConfigBeans configAndMappingConfigBeans, StatsType globalStatsType) {
		StatsType statsType = getStatsType(configAndMappingConfigBeans.getStatsStore(), globalStatsType);

		if (statsType.equals(StatsType.NONE)) {
			return;
		}

		boolean componentLevel = getNotEmptyMappingConfigBeans(configAndMappingConfigBeans.getMappingConfigBeans()) > 0 ? true : false;
		createFactoryAccumulators(configAndMappingConfigBeans.getConfigBean(), statsType, componentLevel);
		createSpecialAccumulators(configAndMappingConfigBeans.getConfigBean(), statsType, componentLevel);
	}

	@Override
	protected void createFactoryAccumulators(ConfigBean configBean, StatsType statsType, boolean componentLevel) {
		FilterByResultConfigBean filterByResultConfigBean = (FilterByResultConfigBean) configBean;

		AnvizentAccumulator inAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), filterByResultConfigBean.getName(),
		        getName(), StatsCategory.IN, StatsNames.IN, true, getStatsCalculator(StatsCategory.IN, StatsNames.IN, getName()));
		ApplicationBean.getInstance().addAccumulator(filterByResultConfigBean.getName(), getName(), inAnvizentAccumulator);

		for (int i = 0; i < filterByResultConfigBean.getEmitStreamNames().size(); i++) {
			String internalRDDName = getName() + "_" + filterByResultConfigBean.getEmitStreamNames().get(i);
			createFactoryAccumulators(filterByResultConfigBean.getName(), internalRDDName, statsType, componentLevel);
		}
	}

	@Override
	protected IStatsCalculator getStatsCalculator(StatsCategory statsCategory, String statsName, String internalRDDName) {
		return new AnvizentFilterStatsCalculator(statsCategory, statsName);
	}

	private void createFactoryAccumulators(String componentName, String internalRDDName, StatsType statsType, boolean componentLevel) {
		AnvizentAccumulator filteredAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), componentName,
		        internalRDDName, StatsCategory.OTHER, StatsNames.FILTERED, true, getStatsCalculator(StatsCategory.OTHER, StatsNames.FILTERED, internalRDDName));
		AnvizentAccumulator errorAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), componentName, internalRDDName,
		        StatsCategory.ERROR, StatsNames.ERROR, true, getStatsCalculator(StatsCategory.ERROR, StatsNames.ERROR, internalRDDName));

		ApplicationBean.getInstance().addAccumulator(componentName, internalRDDName, filteredAnvizentAccumulator, errorAnvizentAccumulator);

		if ((statsType.equals(StatsType.COMPONENT_LEVEL) && !componentLevel) || statsType.equals(StatsType.ALL)) {
			AnvizentAccumulator outAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), componentName,
			        internalRDDName, StatsCategory.OUT, StatsNames.OUT, !componentLevel,
			        getStatsCalculator(StatsCategory.OUT, StatsNames.OUT, internalRDDName));

			ApplicationBean.getInstance().addAccumulator(componentName, internalRDDName, outAnvizentAccumulator);
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
