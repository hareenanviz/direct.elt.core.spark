package com.anvizent.elt.core.spark.sink.factory.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.constant.StoreType;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.spark.common.util.SQLUtil;
import com.anvizent.elt.core.spark.constant.BatchType;
import com.anvizent.elt.core.spark.constant.DBInsertMode;
import com.anvizent.elt.core.spark.exception.InvalidInputForConfigException;
import com.anvizent.elt.core.spark.sink.config.bean.SQLSinkConfigBean;
import com.anvizent.elt.core.spark.sink.function.SQLSinkWithDBCheckBatchFunction;
import com.anvizent.elt.core.spark.sink.function.SQLSinkWithDBCheckFunction;
import com.anvizent.elt.core.spark.sink.function.bean.SQLSinkDBQueryFunctionBean;
import com.anvizent.elt.core.spark.sink.service.SQLSinkService;
import com.anvizent.elt.core.spark.sink.service.SQLUpdateService;

public class SQLSinkFactoryDBQueryUtil {

	private SQLSinkConfigBean sqlSinkConfigBean;
	private LinkedHashMap<String, AnvizentDataType> structure;
	private JavaRDD<HashMap<String, Object>> rdd;

	public SQLSinkFactoryDBQueryUtil(SQLSinkConfigBean sqlSinkConfigBean, LinkedHashMap<String, AnvizentDataType> structure,
	        JavaRDD<HashMap<String, Object>> rdd) {
		this.sqlSinkConfigBean = sqlSinkConfigBean;
		this.structure = structure;
		this.rdd = rdd;
	}

	public void write(ArrayList<AnvizentAccumulator> anvizentAccumulators, AnvizentVoidFunction errorHandlerSinkFunction, JobDetails jobDetails)
	        throws ImproperValidationException, Exception {
		SQLSinkDBQueryFunctionBean functionBean = getFunctionBean();

		if (sqlSinkConfigBean.getBatchType().equals(BatchType.NONE)) {
			rdd.foreach(new SQLSinkWithDBCheckFunction(sqlSinkConfigBean, functionBean, structure, anvizentAccumulators, errorHandlerSinkFunction, jobDetails));
		} else {
			rdd.foreachPartition(new SQLSinkWithDBCheckBatchFunction(sqlSinkConfigBean, functionBean, structure, anvizentAccumulators, errorHandlerSinkFunction,
			        jobDetails));
		}
	}

	private SQLSinkDBQueryFunctionBean getFunctionBean() throws UnimplementedException, InvalidInputForConfigException {
		SQLSinkDBQueryFunctionBean functionBean = new SQLSinkDBQueryFunctionBean(sqlSinkConfigBean, structure);

		if (sqlSinkConfigBean.getDBInsertMode().equals(DBInsertMode.UPSERT)) {
			functionBean.setQueryInfo(SQLUtil.buildUpsertQueryWithConstantFields(sqlSinkConfigBean.getRdbmsConnection().getDriver(), getInsertFields(),
			        sqlSinkConfigBean.getTableName(), sqlSinkConfigBean.getInsertConstantsConfigBean().getColumns(),
			        sqlSinkConfigBean.getInsertConstantsConfigBean().getValues(), sqlSinkConfigBean.getUpdateConstantsConfigBean().getColumns(),
			        sqlSinkConfigBean.getUpdateConstantsConfigBean().getValues(), sqlSinkConfigBean.getMetaDataFields(), sqlSinkConfigBean.getChecksumField(),
			        sqlSinkConfigBean.isAlwaysUpdate()));
		} else {
			functionBean.setQueryInfo(SQLUtil.buildInsertIfNotExistQueryWithConstantFields(sqlSinkConfigBean.getRdbmsConnection().getDriver(),
			        getInsertFields(), sqlSinkConfigBean.getTableName(), sqlSinkConfigBean.getConstantsConfigBean().getColumns(),
			        sqlSinkConfigBean.getConstantsConfigBean().getValues()));
		}

		functionBean.setStoreType(StoreType.getInstance(sqlSinkConfigBean.getRdbmsConnection().getDriver()));
		functionBean.setDeleteQuery(SQLSinkService.buildDeleteQueries(structure.keySet(), sqlSinkConfigBean));
		functionBean.setOnConnectRunQuery(sqlSinkConfigBean.getOnConnectRunQuery());
		functionBean.setDeleteIndicatorField(sqlSinkConfigBean.getDeleteIndicatorField());

		return functionBean;
	}

	private ArrayList<String> getInsertFields() {
		LinkedHashMap<String, AnvizentDataType> structureAfterRemovingDeleteIndicator = new LinkedHashMap<String, AnvizentDataType>(structure);

		if (StringUtils.isNotBlank(sqlSinkConfigBean.getDeleteIndicatorField())) {
			structureAfterRemovingDeleteIndicator.remove(sqlSinkConfigBean.getDeleteIndicatorField());
		}

		return SQLUpdateService.getDifferFields(new ArrayList<>(structureAfterRemovingDeleteIndicator.keySet()), sqlSinkConfigBean.getFieldsDifferToColumns(),
		        sqlSinkConfigBean.getColumnsDifferToFields());
	}
}
