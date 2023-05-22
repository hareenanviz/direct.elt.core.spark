package com.anvizent.elt.core.spark.operation.factory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.constant.Constants.StatsNames;
import com.anvizent.elt.core.lib.constant.StatsCategory;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.lib.stats.calculator.AnvizentStatsCalculator;
import com.anvizent.elt.core.lib.stats.calculator.IStatsCalculator;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.listener.common.constant.StatsType;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.common.util.ErrorHandlerUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.Components;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.ResultFetcher;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.operation.config.bean.ResultFetcherConfigBean;
import com.anvizent.elt.core.spark.operation.doc.helper.ResultFetcherDocHelper;
import com.anvizent.elt.core.spark.operation.function.ResultFetcherFunction;
import com.anvizent.elt.core.spark.operation.schema.validator.ResultFetcherSchemaValidator;
import com.anvizent.elt.core.spark.operation.service.ReflectionUtil;
import com.anvizent.elt.core.spark.operation.validator.ResultFetcherValidator;
import com.anvizent.elt.core.spark.schema.validator.SchemaValidator;
import com.anvizent.elt.core.spark.util.StructureUtil;
import com.anvizent.elt.core.spark.validator.ResourceValidator;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public class ResultFetcherFactory extends SimpleOperationFactory {
	private static final long serialVersionUID = 1L;

	@Override
	protected Component process(ConfigBean configBean, Component component, ErrorHandlerSink errorHandlerSink) throws Exception {
		ResultFetcherConfigBean resultFetcherConfigBean = (ResultFetcherConfigBean) configBean;

		LinkedHashMap<String, AnvizentDataType> newStructure = getNewStructure(resultFetcherConfigBean, component.getStructure());

		JavaRDD<HashMap<String, Object>> expressionRDD = component.getRDD(resultFetcherConfigBean.getSourceStream())
		        .flatMap(new ResultFetcherFunction(resultFetcherConfigBean, component.getStructure(), newStructure,
		                ApplicationBean.getInstance().getAccumulators(resultFetcherConfigBean.getName(), getName()),
		                ErrorHandlerUtil.getErrorHandlerFunction(configBean, component.getStructure(), errorHandlerSink, getName()),
		                ErrorHandlerUtil.getJobDetails(configBean, getName())));

		if (configBean.isPersist()) {
			expressionRDD.persist(StorageLevel.MEMORY_AND_DISK());
		}

		return Component.createComponent(component.getSparkSession(), resultFetcherConfigBean.getName(), expressionRDD, newStructure);
	}

	private LinkedHashMap<String, AnvizentDataType> getNewStructure(ResultFetcherConfigBean resultFetcherConfigBean,
	        LinkedHashMap<String, AnvizentDataType> structure) throws UnsupportedException, InvalidConfigException {

		LinkedHashMap<String, AnvizentDataType> newStructure = new LinkedHashMap<String, AnvizentDataType>(structure);

		ArrayList<AnvizentDataType> newDataTypes = getNewStructureDataTypes(resultFetcherConfigBean, structure);

		return StructureUtil.getNewStructure(resultFetcherConfigBean, newStructure, resultFetcherConfigBean.getReturnFields(), newDataTypes,
		        resultFetcherConfigBean.getReturnFieldsIndexes(), ResultFetcher.RETURN_FIELDS, ResultFetcher.RETURN_FIELDS_INDEXES);
	}

	@SuppressWarnings({ "rawtypes" })
	private ArrayList<AnvizentDataType> getNewStructureDataTypes(ResultFetcherConfigBean resultFetcherConfigBean,
	        LinkedHashMap<String, AnvizentDataType> structure) throws UnsupportedException, InvalidConfigException {
		InvalidConfigException invalidConfigException = generateException(resultFetcherConfigBean);

		ArrayList<AnvizentDataType> anvizentDataTypes = new ArrayList<>();

		try {
			for (int i = 0; i < resultFetcherConfigBean.getReturnFields().size(); i++) {
				Class classType = Class.forName(resultFetcherConfigBean.getClassNames().get(i));
				String methodName = resultFetcherConfigBean.getMethodNames().get(i);
				Class[] methodArgumentTypes = getMethodArguments(resultFetcherConfigBean.getMethodArgumentFields().get(i), structure, invalidConfigException);

				System.out.println("########### fetching method #############");

				Method method = ReflectionUtil
				        .getMethodWithVarArgsIndex(methodArgumentTypes, classType, methodName, resultFetcherConfigBean.getVarArgsIndexes(i)).getMethod();

				System.out.println("########### found method #############" + method);

				anvizentDataTypes.add(new AnvizentDataType(method.getReturnType()));
				resultFetcherConfigBean.addMethodTypes(Modifier.isStatic(method.getModifiers()) ? "static" : "non-static");
				resultFetcherConfigBean.addMethodArgumentFieldTypes(methodArgumentTypes);

				if (resultFetcherConfigBean.getMethodTypes().get(i).equals("static")) {
					resultFetcherConfigBean.putClass(resultFetcherConfigBean.getClassNames().get(i), classType);
				} else {
					resultFetcherConfigBean.putClassObject(resultFetcherConfigBean.getClassNames().get(i), getClassObject(classType));
				}
			}
		} catch (ClassNotFoundException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException
		        | InvocationTargetException exception) {
			invalidConfigException.add(exception.getMessage(), exception);
		}

		if (invalidConfigException.getNumberOfExceptions() > 0) {
			throw invalidConfigException;
		}

		return anvizentDataTypes;
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
		return Components.RESULT_FETCHER.get(General.NAME);
	}

	@Override
	public DocHelper getDocHelper() throws InvalidParameter {
		return new ResultFetcherDocHelper(this);
	}

	@Override
	public Validator getValidator() {
		return new ResultFetcherValidator(this);
	}

	@Override
	public SchemaValidator getSchemaValidator() {
		return new ResultFetcherSchemaValidator();
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
