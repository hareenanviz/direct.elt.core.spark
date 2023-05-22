package com.anvizent.elt.core.spark.operation.function;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
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
import com.anvizent.elt.core.lib.function.AnvizentFunction;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.spark.operation.config.bean.ResultFetcherConfigBean;
import com.anvizent.elt.core.spark.operation.pojo.MethodInfo;
import com.anvizent.elt.core.spark.operation.service.ReflectionUtil;
import com.anvizent.elt.core.spark.util.RowUtil;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ResultFetcherFunction extends AnvizentFunction {

	private static final long serialVersionUID = 1L;
	private ArrayList<MethodInfo> methodInfos;

	public ResultFetcherFunction(ResultFetcherConfigBean resultFetcherConfigBean, LinkedHashMap<String, AnvizentDataType> structure,
	        LinkedHashMap<String, AnvizentDataType> newStructure, ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction errorHandlerSinkFunction,
	        JobDetails jobDetails) throws InvalidArgumentsException {
		super(resultFetcherConfigBean, null, structure, newStructure, null, accumulators, errorHandlerSinkFunction, jobDetails);
	}

	private void initMethodInfos(ResultFetcherConfigBean resultFetcherConfigBean) throws ValidationViolationException {
		if (methodInfos == null) {
			synchronized (this) {
				if (methodInfos == null) {
					methodInfos = new ArrayList<>();

					try {
						for (int i = 0; i < resultFetcherConfigBean.getReturnFields().size(); i++) {
							Class<?> classType;
							Object classObject;

							if (resultFetcherConfigBean.getMethodTypes().get(i).equals("static")) {
								classObject = null;
								classType = resultFetcherConfigBean.getClass(resultFetcherConfigBean.getClassNames().get(i));
							} else {
								classObject = resultFetcherConfigBean.getClassObject(resultFetcherConfigBean.getClassNames().get(i));
								classType = classObject.getClass();
							}

							MethodInfo methodInfo = ReflectionUtil.getMethodWithVarArgsIndex(resultFetcherConfigBean.getMethodArgumentFieldTypes().get(i),
							        classType, resultFetcherConfigBean.getMethodNames().get(i), resultFetcherConfigBean.getVarArgsIndexes(i));
							methodInfo.setObject(classObject);

							methodInfos.add(methodInfo);
						}

					} catch (IllegalArgumentException | NoSuchMethodException |

					        SecurityException exception) {
						throw new ValidationViolationException(exception.getMessage(), exception);
					}
				}
			}
		}
	}

	@Override
	public HashMap<String, Object> process(HashMap<String, Object> row) throws ValidationViolationException, DataCorruptedException, RecordProcessingException {
		ResultFetcherConfigBean resultFetcherConfigBean = (ResultFetcherConfigBean) configBean;
		initMethodInfos(resultFetcherConfigBean);

		HashMap<String, Object> newRow = new HashMap<>();

		try {
			for (int i = 0; i < resultFetcherConfigBean.getReturnFields().size(); i++) {
				Object[] argumentValues = getArgumentValues(resultFetcherConfigBean.getMethodArgumentFields().get(i), row, methodInfos.get(i).getVarArgsIndex(),
				        methodInfos.get(i).getVarArgsType());
				Object returnValue = methodInfos.get(i).getMethod().invoke(methodInfos.get(i).getObject(), argumentValues);
				newRow.put(resultFetcherConfigBean.getReturnFields().get(i), returnValue);
			}
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | SecurityException exception) {
			throw new ValidationViolationException(exception.getMessage(), exception);
		}

		return RowUtil.addElements(row, newRow, newStructure);
	}

	private Object[] getArgumentValues(ArrayList<String> argumentFields, HashMap<String, Object> row, Integer varArgsIndex, Class<?> varArgsType) {
		if (argumentFields == null || argumentFields.isEmpty()) {
			return null;
		} else {
			if (varArgsIndex == null) {
				Object[] argumentValues = new Object[argumentFields.size()];

				for (int i = 0; i < argumentFields.size(); i++) {
					argumentValues[i] = row.get(argumentFields.get(i));
				}

				return argumentValues;
			} else {
				Object[] argumentValues = new Object[varArgsIndex + 1];
				int fieldsIndex = 0;

				for (int i = 0; i < varArgsIndex; i++) {
					argumentValues[i] = row.get(argumentFields.get(fieldsIndex++));
				}

				Object varArgsValue = Array.newInstance(varArgsType, argumentFields.size() - varArgsIndex);

				for (int i = 0; i < argumentFields.size() - varArgsIndex; i++) {
					Array.set(varArgsValue, i, row.get(argumentFields.get(fieldsIndex++)));
				}

				argumentValues[varArgsIndex] = varArgsValue;

				return argumentValues;
			}
		}
	}
}
