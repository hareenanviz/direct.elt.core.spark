package com.anvizent.elt.core.spark.operation.service;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;

import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.ResultFetcher;
import com.anvizent.elt.core.spark.operation.pojo.MethodInfo;

public class ReflectionUtil {

	public static MethodInfo getMethodWithVarArgsIndex(Class<?>[] methodArgumentTypes, Class<?> className, String methodName, Integer varArgsIndex)
	        throws NoSuchMethodException {
		Method[] methods = className.getMethods();
		ArrayList<MethodInfo> matchingMethods = getMatchingMethods(methodArgumentTypes, methods, methodName, varArgsIndex);
		System.out.println("########### matchingMethods.size #############" + matchingMethods.size());

		if (matchingMethods.isEmpty()) {
			throw new NoSuchMethodException("In " + className + ", method: " + methodName + ", with params type: " + Arrays.toString(methodArgumentTypes));
		} else if (matchingMethods.size() > 1) {
			throwAmbiguityException(varArgsIndex, className, methodName, methodArgumentTypes);
			return null;
		} else {
			System.out.println("########### matchingMethods.get(0).method #############" + matchingMethods.get(0).getMethod());
			return matchingMethods.get(0);
		}
	}

	private static void throwAmbiguityException(Integer varArgsIndex, Class<?> className, String methodName, Class<?>[] methodArgumentTypes)
	        throws NoSuchMethodException {
		if (varArgsIndex == null) {
			throw new NoSuchMethodException("Found ambiguity in " + className + ", method: " + methodName + ", with params type: "
			        + Arrays.toString(methodArgumentTypes) + ". Please use " + ResultFetcher.VAR_ARGS_INDEXES
			        + " if you have var args. If there is no varargs please wait for the next version.");
		} else {
			throw new NoSuchMethodException("Found ambiguity in " + className + ", method: " + methodName + ", with params type: "
			        + Arrays.toString(methodArgumentTypes) + ". Please wait for the next version.");
		}
	}

	private static ArrayList<MethodInfo> getMatchingMethods(Class<?>[] methodArgumentTypes, Method[] methods, String methodName, Integer varArgsIndex) {
		ArrayList<MethodInfo> matchingMethods = new ArrayList<>();

		for (int i = 0; i < methods.length; i++) {
			Class<?>[] methodTypes = methods[i].getParameterTypes();
			if (methods[i].getName().equals(methodName) && argsMatching(methodArgumentTypes, methodTypes, varArgsIndex)) {
				MethodInfo methodInfo = new MethodInfo();

				if (isMethodHaveVarArgs(methodTypes)) {
					if (varArgsIndex == null) {
						methodInfo.setVarArgsIndex(methodTypes.length - 1);
					} else {
						methodInfo.setVarArgsIndex(varArgsIndex);
					}

					methodInfo.setVarArgsType(methodTypes[methodTypes.length - 1].getComponentType());
				}

				methodInfo.setMethod(methods[i]);
				matchingMethods.add(methodInfo);
			}
		}

		return matchingMethods;
	}

	private static boolean argsMatching(Class<?>[] typesFromRow, Class<?>[] methodTypes, Integer varArgsIndex) {
		if (methodTypes == null || methodTypes.length == 0) {
			return false;
		}

		if (isMethodHaveVarArgs(methodTypes)) {
			if (varArgsIndex == null || varArgsIndex + 1 == methodTypes.length) {
				return varArgsMatching(typesFromRow, methodTypes);
			} else {
				return false;
			}
		} else if (typesFromRow.length != methodTypes.length) {
			return false;
		} else {
			return argsMatching(typesFromRow, methodTypes);
		}
	}

	private static boolean argsMatching(Class<?>[] typesFromRow, Class<?>[] methodTypes) {
		for (int i = 0; i < typesFromRow.length; i++) {
			if (!methodTypes[i].isAssignableFrom(typesFromRow[i])) {
				return false;
			}
		}

		return true;
	}

	private static boolean varArgsMatching(Class<?>[] typesFromRow, Class<?>[] methodTypes) {
		int i;

		for (i = 0; i < methodTypes.length - 1; i++) {
			if (!methodTypes[i].isAssignableFrom(typesFromRow[i])) {
				return false;
			}
		}

		Class<?> varArgsComponentType = methodTypes[i].getComponentType();

		for (int j = i; j < typesFromRow.length; j++) {
			if (!varArgsComponentType.isAssignableFrom(typesFromRow[i])) {
				return false;
			}
		}

		return true;
	}

	private static boolean isMethodHaveVarArgs(Class<?>[] methodTypes) {
		return Object[].class.isAssignableFrom(methodTypes[methodTypes.length - 1]);
	}
}
