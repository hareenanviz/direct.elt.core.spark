package com.anvizent.elt.core.spark.operation.schema.validator;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeoutException;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.CoerceException;
import com.anvizent.elt.core.lib.exception.DateParseException;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidConfigValueException;
import com.anvizent.elt.core.lib.exception.InvalidSituationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedCoerceException;
import com.anvizent.elt.core.lib.util.TypeConversionUtil;
import com.anvizent.elt.core.lib.util.TypeConversionValidationUtil;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation;
import com.anvizent.elt.core.spark.constant.Constants;
import com.anvizent.elt.core.spark.constant.Constants.General;
import com.anvizent.elt.core.spark.constant.OnZeroFetchOperation;
import com.anvizent.elt.core.spark.constant.SQLTypes;
import com.anvizent.elt.core.spark.exception.InvalidInputForConfigException;
import com.anvizent.elt.core.spark.operation.config.bean.SQLRetrievalConfigBean;
import com.anvizent.elt.core.spark.operation.service.SQLRetrievalFactoryService;
import com.anvizent.elt.core.spark.schema.validator.SchemaValidator;
import com.anvizent.elt.core.spark.util.CollectionUtil;
import com.anvizent.elt.core.spark.util.StructureUtil;

/**
 * @author Hareen Bejjanki
 *
 */
public class SQLRetrievalSchemaValidator extends SchemaValidator {
	private static final long serialVersionUID = 1L;

	@Override
	public void validate(ConfigBean configBean, int sourceIndex, LinkedHashMap<String, AnvizentDataType> structure,
	        InvalidConfigException invalidConfigException)
	        throws ImproperValidationException, UnimplementedException, ClassNotFoundException, SQLException, InvalidInputForConfigException, TimeoutException,
	        DateParseException, UnsupportedCoerceException, InvalidSituationException, InvalidConfigValueException, CoerceException {
		SQLRetrievalConfigBean sqlRetrievalConfigBean = (SQLRetrievalConfigBean) configBean;

		StructureUtil.fieldsNotInSchema(Operation.General.WHERE_FIELDS, sqlRetrievalConfigBean.getWhereFields(), structure, false, invalidConfigException);
		if (sqlRetrievalConfigBean.getOnZeroFetch().equals(OnZeroFetchOperation.INSERT)) {
			StructureUtil.fieldsNotInSchema(Operation.General.INSERT_VALUE_BY_FIELDS, sqlRetrievalConfigBean.getInsertValueByFields(), structure, true,
			        invalidConfigException);
		}

		SQLRetrievalFactoryService.setCustomWhereQuery(sqlRetrievalConfigBean);
		ArrayList<String> selectFields = getSelectFields(sqlRetrievalConfigBean.getSelectColumns(), sqlRetrievalConfigBean.getSelectFieldAliases());
		StructureUtil.fieldsInSchema(Operation.General.SELECT_COLUMNS, selectFields, structure, invalidConfigException, true);

		ArrayList<String>[] columns = getAllColumnsToSelect(sqlRetrievalConfigBean);

		HashMap<String, HashMap<String, ? extends Serializable>> sqlType = SQLRetrievalFactoryService.getSQLTypes(sqlRetrievalConfigBean.getRdbmsConnection(),
		        sqlRetrievalConfigBean.getTableName(), columns[0], columns[1]);

		setAIColumn(sqlRetrievalConfigBean, (String) sqlType.get(Constants.General.AI_COLUMN).get(Constants.General.AI_COLUMN));

		if (sqlRetrievalConfigBean.getOnZeroFetch().equals(OnZeroFetchOperation.INSERT)) {
			boolean hasInsertByFields = CollectionUtil.isAnyNotEmpty(sqlRetrievalConfigBean.getInsertValueByFields());

			setInsertConstantValues(sqlRetrievalConfigBean, sqlType, hasInsertByFields);

			if (hasInsertByFields) {
				validateIfInsertByFieldsCoercable(sqlRetrievalConfigBean, sqlType, structure);
			}
		}
	}

	private void setAIColumn(SQLRetrievalConfigBean sqlRetrievalConfigBean, String aiColumn) {
		if (aiColumn != null) {
			int aliasIndex = CollectionUtil.indexOf(aiColumn, sqlRetrievalConfigBean.getSelectFieldAliases(), false);

			if (aliasIndex != -1) {
				sqlRetrievalConfigBean.setAiColumnIndex(aliasIndex);
			} else {
				int index = sqlRetrievalConfigBean.getSelectColumns().indexOf(aiColumn);

				if (index != -1) {
					sqlRetrievalConfigBean.setAiColumnIndex(index);
				}
			}
		}
	}

	@SuppressWarnings("rawtypes")
	private void validateIfInsertByFieldsCoercable(SQLRetrievalConfigBean sqlRetrievalConfigBean,
	        HashMap<String, HashMap<String, ? extends Serializable>> sqlTypes, LinkedHashMap<String, AnvizentDataType> structure) throws CoerceException {
		HashMap<String, ? extends Serializable> insertValueTypeClasses = sqlTypes.get(Constants.General.JAVA_TYPES);
		HashMap<String, ? extends Serializable> insertValueTypes = sqlTypes.get(Constants.General.SQL_TYPES);
		boolean hasNoDateFormats = CollectionUtil.isAllEmpty(sqlRetrievalConfigBean.getInsertValuesByFieldFormats());

		for (int i = 0; i < sqlRetrievalConfigBean.getInsertValues().size(); i++) {
			if (isAIColumn(sqlRetrievalConfigBean, i) || !isInsertByField(sqlRetrievalConfigBean, i, true)) {
				continue;
			}

			String field;
			if (CollectionUtils.isNotEmpty(sqlRetrievalConfigBean.getSelectFieldAliases())) {
				field = sqlRetrievalConfigBean.getSelectFieldAliases().get(i);
			} else {
				field = sqlRetrievalConfigBean.getSelectColumns().get(i);
			}

			String fieldToRead = sqlRetrievalConfigBean.getInsertValueByFields().get(i);

			Class fromType = structure.get(fieldToRead).getJavaType();
			Class toType = (Class) insertValueTypeClasses.get(field);

			if (insertValueTypes.get(field).equals(SQLTypes.TIMESTAMP) || insertValueTypes.get(field).equals(SQLTypes.TIME)
			        || insertValueTypes.get(field).equals(SQLTypes.DATE)) {
				toType = Date.class;
			} else {
				toType = (Class) insertValueTypeClasses.get(field);
			}

			if (!TypeConversionValidationUtil.canConvertToDataType(fromType, toType)) {
				throw new CoerceException("Cannot coerce from type: " + fromType + ", to type: " + toType + ", for the field: " + field);
			}

			if (fromType.equals(String.class) && toType.equals(Date.class)
			        && (hasNoDateFormats || StringUtils.isBlank(sqlRetrievalConfigBean.getInsertValuesByFieldFormats().get(i)))) {
				throw new CoerceException("Date format is not provided for the field: " + field);
			}
		}
	}

	@SuppressWarnings("rawtypes")
	private void setInsertConstantValues(SQLRetrievalConfigBean sqlRetrievalConfigBean, HashMap<String, HashMap<String, ? extends Serializable>> sqlTypes,
	        boolean hasInsertByFields)
	        throws DateParseException, UnsupportedCoerceException, ImproperValidationException, InvalidConfigValueException, InvalidSituationException {
		HashMap<String, Object> insertValues = new HashMap<>();
		HashMap<String, ? extends Serializable> insertValueTypes = sqlTypes.get(Constants.General.SQL_TYPES);
		HashMap<String, ? extends Serializable> insertValueTypeClasses = sqlTypes.get(Constants.General.JAVA_TYPES);

		for (int i = 0; i < sqlRetrievalConfigBean.getInsertValues().size(); i++) {
			if (isAIColumn(sqlRetrievalConfigBean, i) || isInsertByField(sqlRetrievalConfigBean, i, hasInsertByFields)) {
				continue;
			}

			String field;
			if (CollectionUtils.isNotEmpty(sqlRetrievalConfigBean.getSelectFieldAliases())) {
				field = sqlRetrievalConfigBean.getSelectFieldAliases().get(i);
			} else {
				field = sqlRetrievalConfigBean.getSelectColumns().get(i);
			}

			SQLTypes insertValueType = (SQLTypes) insertValueTypes.get(field);
			Object obj = getValueFromConstant(sqlRetrievalConfigBean.getInsertValues().get(i), insertValueType, (Class) insertValueTypeClasses.get(field));

			insertValues.put(field, obj);
		}

		sqlRetrievalConfigBean.setInsertConstantValues(insertValues);
	}

	@SuppressWarnings("rawtypes")
	private Object getValueFromConstant(String insertValue, SQLTypes insertValueType, Class insertValueTypeClass)
	        throws UnsupportedCoerceException, InvalidSituationException, DateParseException, ImproperValidationException, InvalidConfigValueException {
		Object obj = null;
		if (insertValueType.equals(SQLTypes.TIMESTAMP)) {
			obj = TypeConversionUtil.stringToDateConversion(insertValue, Date.class, General.DATE_FORMAT + " " + General.TIME_FORMAT,
			        Operation.General.INSERT_VALUES);
		} else if (insertValueType.equals(SQLTypes.TIME)) {
			obj = TypeConversionUtil.stringToDateConversion(insertValue, Date.class, General.TIME_FORMAT, Operation.General.INSERT_VALUES);
		} else if (insertValueType.equals(SQLTypes.DATE)) {
			obj = TypeConversionUtil.stringToDateConversion(insertValue, Date.class, General.DATE_FORMAT, Operation.General.INSERT_VALUES);
		} else {
			obj = TypeConversionUtil.stringToOtherTypeConversion(insertValue, insertValueTypeClass, null, null, null);
		}

		return obj;
	}

	private boolean isAIColumn(SQLRetrievalConfigBean sqlRetrievalConfigBean, int i) {
		return sqlRetrievalConfigBean.isUseAIValue() && sqlRetrievalConfigBean.getAiColumnIndex() != null && sqlRetrievalConfigBean.getAiColumnIndex() == i;
	}

	private boolean isInsertByField(SQLRetrievalConfigBean sqlRetrievalConfigBean, int i, boolean hasInsertByFields) {
		return hasInsertByFields && sqlRetrievalConfigBean.getInsertValueByFields().get(i) != null
		        && !sqlRetrievalConfigBean.getInsertValueByFields().get(i).isEmpty();
	}

	@SuppressWarnings("unchecked")
	private ArrayList<String>[] getAllColumnsToSelect(SQLRetrievalConfigBean sqlRetrievalConfigBean) {
		ArrayList<String> selectColumns = new ArrayList<>(sqlRetrievalConfigBean.getSelectColumns());
		ArrayList<String> selectFieldAliases;

		if (sqlRetrievalConfigBean.getSelectFieldAliases() == null || sqlRetrievalConfigBean.getSelectFieldAliases().isEmpty()) {
			selectFieldAliases = new ArrayList<>(selectColumns);
		} else {
			selectFieldAliases = new ArrayList<>(sqlRetrievalConfigBean.getSelectFieldAliases());
		}

		if (StringUtils.isEmpty(sqlRetrievalConfigBean.getCustomWhere())) {
			if (sqlRetrievalConfigBean.getWhereColumns() != null && !sqlRetrievalConfigBean.getWhereColumns().isEmpty()) {
				selectColumns.addAll(sqlRetrievalConfigBean.getWhereColumns());
			} else if (sqlRetrievalConfigBean.getWhereFields() != null && !sqlRetrievalConfigBean.getWhereFields().isEmpty()) {
				selectColumns.addAll(sqlRetrievalConfigBean.getWhereFields());
			}

			if (sqlRetrievalConfigBean.getWhereFields() != null && !sqlRetrievalConfigBean.getWhereFields().isEmpty()) {
				selectFieldAliases.addAll(sqlRetrievalConfigBean.getWhereFields());
			}
		}

		if (sqlRetrievalConfigBean.getOrderBy() != null && !sqlRetrievalConfigBean.getOrderBy().isEmpty()) {
			selectColumns.addAll(sqlRetrievalConfigBean.getOrderBy());
			selectFieldAliases.addAll(sqlRetrievalConfigBean.getOrderBy());
		}

		ArrayList<String>[] result = new ArrayList[2];
		result[0] = selectColumns;
		result[1] = selectFieldAliases;

		return result;
	}

	private ArrayList<String> getSelectFields(ArrayList<String> selectFields, ArrayList<String> selectFieldAliases) {
		if (selectFieldAliases == null || selectFieldAliases.isEmpty()) {
			return selectFields;
		} else {
			return selectFieldAliases;
		}
	}

}