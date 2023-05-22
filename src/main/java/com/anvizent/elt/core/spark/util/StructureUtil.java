package com.anvizent.elt.core.spark.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;
import com.anvizent.elt.core.lib.config.bean.SeekDetails;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.SchemaValidationConstant.Message;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant;

/**
 * @author Hareen Bejjanki
 *
 */
public class StructureUtil implements Serializable {
	private static final long serialVersionUID = 1L;

	public static LinkedHashMap<String, AnvizentDataType> getNewStructure(ConfigBean configBean, LinkedHashMap<String, AnvizentDataType> structure,
	        ArrayList<String> newFields, ArrayList<AnvizentDataType> newTypes, ArrayList<Integer> indexes, String fieldsConfigName, String positionsConfigName)
	        throws InvalidConfigException {
		return getNewStructure(configBean.getConfigName(), configBean.getName(), configBean.getSeekDetails(), structure, newFields, newTypes, indexes, null,
		        null, fieldsConfigName, null, positionsConfigName, null, false);
	}

	public static LinkedHashMap<String, AnvizentDataType> getNewStructure(MappingConfigBean configBean, LinkedHashMap<String, AnvizentDataType> structure,
	        ArrayList<String> newFields, ArrayList<AnvizentDataType> newTypes, ArrayList<Integer> indexes, String fieldsConfigName, String positionsConfigName)
	        throws InvalidConfigException {
		return getNewStructure(configBean.getFullConfigName(), configBean.getComponentName(), configBean.getSeekDetails(), structure, newFields, newTypes,
		        indexes, null, null, fieldsConfigName, null, positionsConfigName, null, false);
	}

	public static LinkedHashMap<String, AnvizentDataType> getNewStructure(ConfigBean configBean, LinkedHashMap<String, AnvizentDataType> structure,
	        ArrayList<String> existingFieldsToReposition, ArrayList<Integer> existingFieldsIndexes, String fieldsConfigName, String positionsConfigName)
	        throws InvalidConfigException {
		return getNewStructure(configBean.getConfigName(), configBean.getName(), configBean.getSeekDetails(), structure, null, null, null,
		        existingFieldsToReposition, existingFieldsIndexes, null, fieldsConfigName, null, positionsConfigName, false);
	}

	public static LinkedHashMap<String, AnvizentDataType> getNewStructure(MappingConfigBean configBean, LinkedHashMap<String, AnvizentDataType> structure,
	        ArrayList<String> existingFieldsToReposition, ArrayList<Integer> existingFieldsIndexes, String fieldsConfigName, String positionsConfigName)
	        throws InvalidConfigException {
		return getNewStructure(configBean.getFullConfigName(), configBean.getComponentName(), configBean.getSeekDetails(), structure, null, null, null,
		        existingFieldsToReposition, existingFieldsIndexes, null, fieldsConfigName, null, positionsConfigName, false);
	}

	public static LinkedHashMap<String, AnvizentDataType> getNewStructure(String config, String component, SeekDetails seekDetails,
	        LinkedHashMap<String, AnvizentDataType> structure, ArrayList<String> newFields, ArrayList<AnvizentDataType> newTypes,
	        ArrayList<Integer> newFieldPositions, ArrayList<String> existingFieldsToReposition, ArrayList<Integer> existingFieldsIndexes,
	        String newFieldsConfigName, String existingFieldsConfigName, String newFieldPositionsConfigName, String existingFieldPositionsConfigName,
	        boolean retainOnlySpecifiedFields) throws InvalidConfigException {
		validate(config, component, seekDetails, structure, newFields, newFieldPositions, existingFieldsToReposition, existingFieldsIndexes,
		        newFieldsConfigName, existingFieldsConfigName, newFieldPositionsConfigName, existingFieldPositionsConfigName, retainOnlySpecifiedFields);
		LinkedHashMap<String, AnvizentDataType> newStructure = new LinkedHashMap<>();

		int newSize = structure.size() + (newFields == null ? 0 : newFields.size());

		TreeMap<Integer, Integer> sortedIndexes = getSortedIndexes(newFieldPositions, newSize, structure.size(), true);
		TreeMap<Integer, Integer> existingFieldsSortedIndexes = getSortedIndexes(existingFieldsIndexes, newSize, structure.size(), false);

		addElements(newStructure, structure, sortedIndexes, existingFieldsSortedIndexes, newSize, newFields, newTypes, existingFieldsToReposition,
		        retainOnlySpecifiedFields);

		validateLeftOver(config, component, seekDetails, sortedIndexes, existingFieldsSortedIndexes, newFieldPositionsConfigName,
		        existingFieldPositionsConfigName);

		return newStructure;
	}

	private static void validate(String config, String component, SeekDetails seekDetails, LinkedHashMap<String, AnvizentDataType> structure,
	        ArrayList<String> newFields, ArrayList<Integer> newFieldPositions, ArrayList<String> existingFieldsToReposition,
	        ArrayList<Integer> existingFieldsIndexes, String newFieldsConfigName, String existingFieldsConfigName, String newFieldPositionsConfigName,
	        String existingFieldPositionsConfigName, boolean retainOnlySpecifiedFields) throws InvalidConfigException {
		validateFields(config, component, seekDetails, structure, newFields, existingFieldsToReposition, newFieldsConfigName, existingFieldsConfigName,
		        retainOnlySpecifiedFields);
		validateCommonPositions(config, component, seekDetails, newFieldPositions, existingFieldsIndexes, newFieldPositionsConfigName,
		        existingFieldPositionsConfigName);
	}

	private static void validateFields(String config, String component, SeekDetails seekDetails, LinkedHashMap<String, AnvizentDataType> structure,
	        ArrayList<String> newFields, ArrayList<String> existingFieldsToReposition, String newFieldsConfigName, String existingFieldsConfigName,
	        boolean retainOnlySpecifiedFields) throws InvalidConfigException {
		if (newFields != null && newFields.size() > 0) {
			validateNewFields(config, component, seekDetails, structure, newFields, existingFieldsToReposition, newFieldsConfigName, retainOnlySpecifiedFields);
		}

		if (existingFieldsToReposition != null && existingFieldsToReposition.size() > 0) {
			validateExistingFieldsFields(config, component, seekDetails, structure, existingFieldsToReposition, existingFieldsConfigName);
		}
	}

	private static void validateExistingFieldsFields(String config, String component, SeekDetails seekDetails,
	        LinkedHashMap<String, AnvizentDataType> structure, ArrayList<String> existingFieldsToReposition, String existingFieldsConfigName)
	        throws InvalidConfigException {
		String fieldsNotFound = "";

		for (String existingField : existingFieldsToReposition) {
			if (!structure.containsKey(existingField)) {
				if (!fieldsNotFound.isEmpty()) {
					fieldsNotFound += ", ";
				}

				fieldsNotFound += existingField;
			}
		}

		if (!fieldsNotFound.isEmpty()) {
			InvalidConfigException invalidConfigException = getInvalidConfigException(config, component, seekDetails);

			invalidConfigException.add(ValidationConstant.Message.FIELDS_NOT_FOUND, existingFieldsConfigName);

			throw invalidConfigException;
		}
	}

	private static void validateNewFields(String config, String component, SeekDetails seekDetails, LinkedHashMap<String, AnvizentDataType> structure,
	        ArrayList<String> newFields, ArrayList<String> existingFieldsToReposition, String newFieldsConfigName, boolean retainOnlySpecifiedFields)
	        throws InvalidConfigException {
		String fieldsAlreadyExist = "";

		for (String newField : newFields) {
			if (isFieldAlreadyExists(structure, newField, existingFieldsToReposition, retainOnlySpecifiedFields)) {
				if (!fieldsAlreadyExist.isEmpty()) {
					fieldsAlreadyExist += ", ";
				}

				fieldsAlreadyExist += newField;
			}
		}

		if (!fieldsAlreadyExist.isEmpty()) {
			InvalidConfigException invalidConfigException = getInvalidConfigException(config, component, seekDetails);

			invalidConfigException.add(ValidationConstant.Message.FIELDS_ALREADY_EXIST, newFieldsConfigName);

			throw invalidConfigException;
		}
	}

	private static boolean isFieldAlreadyExists(LinkedHashMap<String, AnvizentDataType> structure, String newField,
	        ArrayList<String> existingFieldsToReposition, boolean retainOnlySpecifiedFields) {
		if (retainOnlySpecifiedFields) {
			return existingFieldsToReposition.contains(newField);
		} else {
			return structure.containsKey(newField);
		}
	}

	private static void validateLeftOver(String config, String component, SeekDetails seekDetails, TreeMap<Integer, Integer> sortedIndexes,
	        TreeMap<Integer, Integer> existingFieldsSortedIndexes, String newFieldPositionsConfigName, String existingFieldPositionsConfigName)
	        throws InvalidConfigException {
		if (sortedIndexes.size() > 0 || existingFieldsSortedIndexes.size() > 0) {
			InvalidConfigException invalidConfigException = getInvalidConfigException(config, component, seekDetails);

			if (sortedIndexes.size() > 0) {
				invalidConfigException.add(ValidationConstant.Message.INVALID_INDEX_VALUES, newFieldPositionsConfigName);
			}

			if (existingFieldsSortedIndexes.size() > 0) {
				invalidConfigException.add(ValidationConstant.Message.INVALID_INDEX_VALUES, existingFieldPositionsConfigName);
			}

			throw invalidConfigException;
		}
	}

	private static void validateCommonPositions(String config, String component, SeekDetails seekDetails, ArrayList<Integer> newFieldPositions,
	        ArrayList<Integer> existingFieldsIndexes, String newFieldPositionsConfigName, String existingFieldPositionsConfigName)
	        throws InvalidConfigException {
		if (CollectionUtil.hasAnyCommonElements(newFieldPositions, existingFieldsIndexes)) {
			InvalidConfigException invalidConfigException = getInvalidConfigException(config, component, seekDetails);

			invalidConfigException.add(ValidationConstant.Message.CAN_NOT_HAVE_COMMON_VALUES, newFieldPositionsConfigName, existingFieldPositionsConfigName);

			throw invalidConfigException;
		}
	}

	private static InvalidConfigException getInvalidConfigException(String config, String component, SeekDetails seekDetails) {
		InvalidConfigException invalidConfigException = new InvalidConfigException();

		invalidConfigException.setComponent(config);
		invalidConfigException.setComponentName(component);
		invalidConfigException.setSeekDetails(seekDetails);

		return invalidConfigException;
	}

	private static void addElements(LinkedHashMap<String, AnvizentDataType> newStructure, LinkedHashMap<String, AnvizentDataType> structure,
	        TreeMap<Integer, Integer> sortedIndexes, TreeMap<Integer, Integer> existingFieldsSortedIndexes, int newSize, ArrayList<String> newFields,
	        ArrayList<AnvizentDataType> newValues, ArrayList<String> existingFieldsToReposition, boolean retainOnlySpecifiedFields) {
		LinkedHashMap<String, AnvizentDataType> removed = CollectionUtil.removeAll(structure, existingFieldsToReposition);
		Iterator<Entry<String, AnvizentDataType>> iterator = structure.entrySet().iterator();

		for (int i = 0; i < newSize; i++) {
			if (sortedIndexes.containsKey(i)) {
				int index = sortedIndexes.remove(i);
				newStructure.put(newFields.get(index), newValues.get(index));
			} else if (existingFieldsSortedIndexes.containsKey(i)) {
				String key = existingFieldsToReposition.get(existingFieldsSortedIndexes.remove(i));
				newStructure.put(key, removed.get(key));
			} else if (!retainOnlySpecifiedFields) {
				Entry<String, AnvizentDataType> data = iterator.next();
				newStructure.put(data.getKey(), data.getValue());
			}
		}
	}

	public static TreeMap<Integer, Integer> getSortedIndexes(ArrayList<Integer> indexes, int newSize, int oldSize, boolean generateNew) {
		TreeMap<Integer, Integer> sortedIndexes = new TreeMap<>();

		if (indexes != null && indexes.size() > 0) {
			for (int i = 0; i < indexes.size(); i++) {
				if (indexes.get(i) < 0) {
					sortedIndexes.put(newSize + indexes.get(i), i);
				} else {
					sortedIndexes.put(indexes.get(i), i);
				}
			}
		} else if (generateNew) {
			for (int i = 0; i < newSize - oldSize; i++) {
				sortedIndexes.put(i + oldSize, i);
			}
		}

		return sortedIndexes;
	}

	public static ArrayList<AnvizentDataType> getNewStructureDataTypes(ArrayList<Class<?>> types, ArrayList<Integer> precisions, ArrayList<Integer> scales)
	        throws UnsupportedException {
		ArrayList<AnvizentDataType> newStructureDataTypes = new ArrayList<>();

		for (int i = 0; i < types.size(); i++) {
			addNewStructureDataType(i, newStructureDataTypes, precisions, scales, types);
		}

		return newStructureDataTypes;
	}

	private static void addNewStructureDataType(int i, ArrayList<AnvizentDataType> newStructureDataTypes, ArrayList<Integer> precisions,
	        ArrayList<Integer> scales, ArrayList<Class<?>> types) throws UnsupportedException {
		Integer precision = CollectionUtil.getFromList(precisions, i, General.DECIMAL_PRECISION);
		Integer scale = CollectionUtil.getFromList(scales, i, General.DECIMAL_SCALE);
		newStructureDataTypes.add(new AnvizentDataType(types.get(i), precision, scale));
	}

	public static void multiLevelfieldsNotInSchema(String config, ArrayList<ArrayList<String>> fields, LinkedHashMap<String, AnvizentDataType> structure,
	        InvalidConfigException invalidConfigException) {
		if (fields != null && !fields.isEmpty()) {
			for (ArrayList<String> field : fields) {
				fieldsNotInSchema(config, field, structure, invalidConfigException);
			}
		}
	}

	public static void fieldsNotInSchema(String config, ArrayList<String> fields, LinkedHashMap<String, AnvizentDataType> structure,
	        InvalidConfigException invalidConfigException) {
		if (fields != null && !fields.isEmpty()) {
			for (String field : fields) {
				if (!structure.containsKey(field)) {
					invalidConfigException.add(Message.FIELD_NOT_IN_SCHEMA, field, config);
				}
			}
		}
	}

	public static void fieldNotInSchema(String config, String field, LinkedHashMap<String, AnvizentDataType> structure,
	        InvalidConfigException invalidConfigException) {
		if (field != null && !field.isEmpty() && !structure.containsKey(field)) {
			invalidConfigException.add(Message.FIELD_NOT_IN_SCHEMA, field, config);
		}
	}

	public static void fieldTypeMatch(String config, String field, Class<?> type, LinkedHashMap<String, AnvizentDataType> structure,
	        InvalidConfigException invalidConfigException) {
		AnvizentDataType deleteFieldType = structure.get(field);
		if (deleteFieldType != null && !deleteFieldType.getJavaType().equals(type)) {
			invalidConfigException.add(Message.TYPE_MISS_MATCH, config, type);
		}
	}

	public static void fieldsNotInSchema(String config, ArrayList<String> fields, LinkedHashMap<String, AnvizentDataType> structure, boolean ignoreEmpty,
	        InvalidConfigException invalidConfigException) {
		if (fields != null && !fields.isEmpty()) {
			for (String field : fields) {
				if (ignoreEmpty) {
					if (field != null && !field.isEmpty() && !structure.containsKey(field)) {
						invalidConfigException.add(Message.FIELD_NOT_IN_SCHEMA, field, config);
					}
				} else if (!structure.containsKey(field)) {
					invalidConfigException.add(Message.FIELD_NOT_IN_SCHEMA, field, config);
				}
			}
		}
	}

	public static void fieldsInSchema(String config, ArrayList<String> fields, LinkedHashMap<String, AnvizentDataType> structure,
	        InvalidConfigException invalidConfigException, boolean caseSensitive) {
		if (fields != null && !fields.isEmpty()) {
			HashSet<String> fieldSet = new HashSet<String>();

			for (String field : structure.keySet()) {
				if (!caseSensitive) {
					fieldSet.add(field.toUpperCase());
				} else {
					fieldSet.add(field);
				}
			}

			for (String field : fields) {
				if ((!caseSensitive && fieldSet.contains(field.toUpperCase())) || (caseSensitive && fieldSet.contains(field))) {
					invalidConfigException.add(Message.FIELD_IN_SCHEMA, field, config);
				}
			}
		}
	}

	public static void typesMissMatchInSchema(String config1, String config2, ArrayList<String> fields1, ArrayList<String> fields2,
	        LinkedHashMap<String, AnvizentDataType> structure, boolean ignoreEmptyFieldNames, InvalidConfigException invalidConfigException) {
		if (fields1 != null && !fields1.isEmpty() && fields2 != null && !fields2.isEmpty()) {
			for (int i = 0; i < fields1.size() && i < fields2.size(); i++) {
				if (ignoreEmptyFieldNames) {
					if (fields1.get(i) != null && !fields1.get(i).isEmpty() && fields2.get(i) != null && !fields2.get(i).isEmpty()) {
						if (!structure.get(fields1.get(i)).equals(structure.get(fields2.get(i)))) {
							invalidConfigException.add(Message.TYPE_MISS_MATCH_FOR_OTHER_FIELD, fields1.get(i), fields2.get(i), config1, config2);
						}
					}
				} else {
					if (fields1.get(i) == null || fields1.get(i).isEmpty() || fields2.get(i) == null || fields2.get(i).isEmpty()) {
						invalidConfigException.add(Message.TYPE_MISS_MATCH_FOR_OTHER_FIELD, fields1, fields2, config1, config2);
					} else {
						if (!structure.get(fields1.get(i)).equals(structure.get(fields2.get(i)))) {
							invalidConfigException.add(Message.TYPE_MISS_MATCH_FOR_OTHER_FIELD, fields1.get(i), fields2.get(i), config1, config2);
						}
					}
				}
			}
		}
	}

}
