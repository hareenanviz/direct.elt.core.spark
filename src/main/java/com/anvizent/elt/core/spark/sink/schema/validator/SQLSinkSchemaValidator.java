package com.anvizent.elt.core.spark.sink.schema.validator;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeoutException;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.TaskContext;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.listener.common.connection.ApplicationConnectionBean;
import com.anvizent.elt.core.listener.common.connection.RDBMSConnectionByTaskId;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Sink;
import com.anvizent.elt.core.spark.constant.DBCheckMode;
import com.anvizent.elt.core.spark.schema.validator.SchemaValidator;
import com.anvizent.elt.core.spark.sink.config.bean.DBConstantsConfigBean;
import com.anvizent.elt.core.spark.sink.config.bean.SQLSinkConfigBean;
import com.anvizent.elt.core.spark.util.StructureUtil;

/**
 * @author Hareen Bejjanki
 *
 */
public class SQLSinkSchemaValidator extends SchemaValidator {
	private static final long serialVersionUID = 1L;

	@Override
	public void validate(ConfigBean configBean, int sourceIndex, LinkedHashMap<String, AnvizentDataType> structure,
	        InvalidConfigException invalidConfigException) throws ImproperValidationException, UnimplementedException, SQLException, TimeoutException {
		SQLSinkConfigBean sinkConfigBean = (SQLSinkConfigBean) configBean;

		StructureUtil.fieldsNotInSchema(Sink.General.KEY_FIELDS, sinkConfigBean.getKeyFields(), structure, invalidConfigException);

		StructureUtil.fieldNotInSchema(Sink.General.DELETE_INDICATOR_FIELD, sinkConfigBean.getDeleteIndicatorField(), structure, invalidConfigException);
		StructureUtil.fieldTypeMatch(Sink.General.DELETE_INDICATOR_FIELD, sinkConfigBean.getDeleteIndicatorField(), Boolean.class, structure,
		        invalidConfigException);

		StructureUtil.fieldsNotInSchema(Sink.General.FIELD_NAMES_DIFFER_TO_COLUMNS, sinkConfigBean.getFieldsDifferToColumns(), structure,
		        invalidConfigException);
		StructureUtil.fieldsNotInSchema(Sink.General.META_DATA_FIELDS, sinkConfigBean.getMetaDataFields(), structure, invalidConfigException);
		constantsInSchema(Sink.SQLSink.CONSTANT_COLUMNS, sinkConfigBean.getConstantsConfigBean(), structure, invalidConfigException, true);
		constantsInSchema(Sink.SQLSink.INSERT_CONSTANT_COLUMNS, sinkConfigBean.getInsertConstantsConfigBean(), structure, invalidConfigException, true);
		constantsInSchema(Sink.SQLSink.UPDATE_CONSTANT_COLUMNS, sinkConfigBean.getUpdateConstantsConfigBean(), structure, invalidConfigException, true);

		setKeyFieldsAndColumns(sinkConfigBean);
	}

	private void setKeyFieldsAndColumns(SQLSinkConfigBean sinkConfigBean)
	        throws ImproperValidationException, UnimplementedException, SQLException, TimeoutException {
		if (StringUtils.isNotBlank(sinkConfigBean.getDeleteIndicatorField()) && sinkConfigBean.getDBCheckMode().equals(DBCheckMode.DB_QUERY)) {
			ArrayList<String> keyColumns = getPKFields(sinkConfigBean);

			if (!keyColumns.isEmpty()) {
				if (CollectionUtils.isEmpty(sinkConfigBean.getFieldsDifferToColumns())) {
					sinkConfigBean.setKeyFields(keyColumns);
				} else {
					sinkConfigBean.setKeyFields(getKeyFields(keyColumns, sinkConfigBean.getFieldsDifferToColumns(), sinkConfigBean.getColumnsDifferToFields()));
					sinkConfigBean.setKeyColumns(keyColumns);
				}
			}
		}
	}

	private ArrayList<String> getKeyFields(ArrayList<String> keyColumns, ArrayList<String> fieldsDifferToColumns, ArrayList<String> columnsDifferToFields) {
		ArrayList<String> keyFields = new ArrayList<String>(keyColumns.size());

		for (int i = 0; i < keyColumns.size(); i++) {
			int index = columnsDifferToFields.indexOf(keyColumns.get(i));

			if (index != -1) {
				keyFields.add(fieldsDifferToColumns.get(index));
			} else {
				keyFields.add(keyColumns.get(i));
			}
		}

		return keyFields;
	}

	private ArrayList<String> getPKFields(SQLSinkConfigBean sinkConfigBean)
	        throws ImproperValidationException, UnimplementedException, SQLException, TimeoutException {
		ArrayList<String> keyColumns = new ArrayList<>();

		DatabaseMetaData meta = ((Connection) ApplicationConnectionBean.getInstance()
		        .get(new RDBMSConnectionByTaskId(sinkConfigBean.getRdbmsConnection(), null, TaskContext.getPartitionId()), true)[0]).getMetaData();
		ResultSet resultSet = meta.getTables(null, null, "IL_Product", new String[] { "TABLE" });
		resultSet = meta.getPrimaryKeys(null, null, "IL_Product");

		while (resultSet.next()) {
			keyColumns.add(resultSet.getString("COLUMN_NAME"));
		}

		return keyColumns;
	}

	private void constantsInSchema(String columnsConfig, DBConstantsConfigBean constantsConfigBean, LinkedHashMap<String, AnvizentDataType> structure,
	        InvalidConfigException invalidConfigException, boolean b) {
		if (constantsConfigBean != null) {
			StructureUtil.fieldsInSchema(columnsConfig, constantsConfigBean.getColumns(), structure, invalidConfigException, false);
		}
	}

}