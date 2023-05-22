package com.anvizent.elt.core.spark.filter.function;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.exception.DataCorruptedException;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.lib.function.AnvizentFilterFunction;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.spark.filter.config.bean.FilterByResultConfigBean;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class FilterByResultFunction extends AnvizentFilterFunction {

	private static final long serialVersionUID = 1L;

	private Integer index;

	public FilterByResultFunction(FilterByResultConfigBean filterByResultConfigBean, Integer index, LinkedHashMap<String, AnvizentDataType> structure,
			LinkedHashMap<String, AnvizentDataType> newStructure, ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction errorHandlerSinkFunction,
			JobDetails jobDetails) throws InvalidArgumentsException {
		super(filterByResultConfigBean, null, structure, newStructure, null, accumulators, errorHandlerSinkFunction, jobDetails);
		this.index = index;
	}

	@Override
	public Boolean process(HashMap<String, Object> row) throws ValidationViolationException, DataCorruptedException, RecordProcessingException {
		FilterByResultConfigBean filterByResultConfigBean = (FilterByResultConfigBean) configBean;

		try {
			ArrayList<Boolean> results = new ArrayList<>(filterByResultConfigBean.getDecodedMethods().get(index).size());

			invokeMethods(row, filterByResultConfigBean, filterByResultConfigBean.getDecodedClassNames().get(index),
					filterByResultConfigBean.getDecodedMethods().get(index), filterByResultConfigBean.getDecodedMethodArguments().get(index),
					filterByResultConfigBean.getDecodedMethodTypes().get(index), filterByResultConfigBean.getDecodedMethodArgumentTypes().get(index),
					filterByResultConfigBean.getDecodedMethodResultTypes().get(index), results);

			return !results.contains(false);
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException exception) {
			throw new ValidationViolationException(exception.getMessage(), exception);
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void invokeMethods(HashMap<String, Object> row, FilterByResultConfigBean filterByResultConfigBean, ArrayList<String> classNames,
			ArrayList<String> methodNames, ArrayList<ArrayList<String>> methodArguments, ArrayList<String> methodTypes, ArrayList<Class[]> methodArgumentTypes,
			ArrayList<String> methodResultTypes, ArrayList<Boolean> results)
			throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		boolean returnValue = false;

		for (int i = 0; i < methodNames.size(); i++) {
			Object[] argumentValues = getArgumentValues(methodArguments.get(i), row);

			if (methodTypes.get(i).equals("static")) {
				Class className = filterByResultConfigBean.getClass(classNames.get(i));
				Method method = className.getMethod(methodNames.get(i), methodArgumentTypes.get(i));
				returnValue = (boolean) method.invoke(null, argumentValues);
			} else {
				Object classObject = filterByResultConfigBean.getClassObject(classNames.get(i));
				Method method = classObject.getClass().getMethod(methodNames.get(i), methodArgumentTypes.get(i));
				returnValue = (boolean) method.invoke(classObject, argumentValues);
			}

			if (methodResultTypes.get(i).equals("NOT_LIKE")) {
				returnValue = !returnValue;
			}

			results.add(returnValue);
		}
	}

	private Object[] getArgumentValues(ArrayList<String> argumentFields, HashMap<String, Object> row) {
		if (argumentFields == null || argumentFields.isEmpty()) {
			return null;
		} else {
			Object[] argumentValues = new Object[argumentFields.size()];
			for (int i = 0; i < argumentFields.size(); i++) {
				argumentValues[i] = row.get(argumentFields.get(i));
			}

			return argumentValues;
		}
	}
}