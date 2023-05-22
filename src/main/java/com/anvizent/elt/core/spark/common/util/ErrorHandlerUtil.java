package com.anvizent.elt.core.spark.common.util;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;

import org.apache.commons.lang3.StringUtils;

import com.anvizent.elt.commons.constants.Constants.ELTConstants;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.config.bean.SeekDetails;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.App;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General.ErrorHandlers;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Sink.Components;
import com.anvizent.elt.core.spark.sink.config.bean.ArangoDBSinkConfigBean;
import com.anvizent.elt.core.spark.sink.config.bean.NoSQLConstantsConfigBean;
import com.anvizent.elt.core.spark.sink.config.bean.RethinkDBSinkConfigBean;
import com.anvizent.elt.core.spark.sink.config.bean.SQLSinkConfigBean;
import com.anvizent.elt.core.spark.sink.function.ArangoDBSinkInsertFunction;
import com.anvizent.elt.core.spark.sink.function.RethinkDBSinkInsertFunction;
import com.anvizent.elt.core.spark.sink.function.SQLSinkInsertFunction;
import com.anvizent.elt.core.spark.sink.service.ArangoDBSinkService;
import com.anvizent.elt.core.spark.sink.service.RethinkDBSinkService;
import com.anvizent.elt.core.spark.sink.service.SQLSinkService;
import com.anvizent.elt.core.spark.sink.validator.ArangoDBSinkValidator;
import com.anvizent.elt.core.spark.sink.validator.ConsoleSinkValidator;
import com.anvizent.elt.core.spark.sink.validator.RethinkDBSinkValidator;
import com.anvizent.elt.core.spark.sink.validator.SQLSinkValidator;
import com.anvizent.elt.core.spark.source.config.bean.ConsoleSinkConfigBean;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ErrorHandlerUtil {

	public static AnvizentVoidFunction getErrorHandlerFunction(ConfigBean configBean, LinkedHashMap<String, AnvizentDataType> structure,
	        ErrorHandlerSink errorHandlerSink, String internalRDDName) throws Exception {
		if (errorHandlerSink == null) {
			return null;
		} else if (errorHandlerSink instanceof RethinkDBSinkConfigBean) {
			RethinkDBSinkConfigBean sinkConfigBean = (RethinkDBSinkConfigBean) errorHandlerSink;

			LinkedHashMap<String, AnvizentDataType> newStructure = createEHStructure(structure);
			putConstantsStructure(sinkConfigBean.getConstantsConfigBean(), newStructure);
			createTableIfNotExists(sinkConfigBean);

			RethinkDBSinkService.closeConnection(sinkConfigBean.getConnection());

			return new RethinkDBSinkInsertFunction((RethinkDBSinkConfigBean) errorHandlerSink, structure, newStructure, null, null, null);
		} else if (errorHandlerSink instanceof ArangoDBSinkConfigBean) {
			ArangoDBSinkConfigBean sinkConfigBean = (ArangoDBSinkConfigBean) errorHandlerSink;

			LinkedHashMap<String, AnvizentDataType> newStructure = createEHStructure(structure);
			putConstantsStructure(sinkConfigBean.getConstantsConfigBean(), newStructure);
			createTableIfNotExists(sinkConfigBean);

			ArangoDBSinkService.closeConnection(sinkConfigBean.getConnection());

			return new ArangoDBSinkInsertFunction((ArangoDBSinkConfigBean) errorHandlerSink, structure, newStructure, null, null, null);
		} else if (errorHandlerSink instanceof SQLSinkConfigBean) {
			SQLSinkConfigBean sinkConfigBean = (SQLSinkConfigBean) errorHandlerSink;

			LinkedHashMap<String, AnvizentDataType> newStructure = createEHStructure(structure);
			putSQLConstants(sinkConfigBean.getConstantsConfigBean().getColumns(), sinkConfigBean.getConstantsConfigBean().getTypes(), newStructure);
			SQLSinkService.setFields(sinkConfigBean, newStructure);
			SQLSinkService.buildInsertQuery(sinkConfigBean, sinkConfigBean.getConstantsConfigBean().getColumns(),
			        sinkConfigBean.getConstantsConfigBean().getValues());
			createTableIfNotExists(sinkConfigBean, newStructure);

			return new SQLSinkInsertFunction(sinkConfigBean, newStructure, null, null, null);
		} else {
			throw new UnimplementedException();
		}
	}

	private static void createTableIfNotExists(RethinkDBSinkConfigBean rethinkDBSinkConfigBean) throws Exception {
		RethinkDBSinkService.setTimeZoneDetails(rethinkDBSinkConfigBean);
		boolean tableExists = RethinkDBSinkService.checkIfTableExists(rethinkDBSinkConfigBean);
		if (!tableExists) {
			RethinkDBSinkService.createTable(rethinkDBSinkConfigBean);
		}
	}

	private static void createTableIfNotExists(ArangoDBSinkConfigBean rethinkDBSinkConfigBean) throws Exception {
//		TODO timezone
//		ArangoDBSinkService.setTimeZoneDetails(rethinkDBSinkConfigBean);
		boolean tableExists = ArangoDBSinkService.checkIfTableExists(rethinkDBSinkConfigBean);
		if (!tableExists) {
			ArangoDBSinkService.createTable(rethinkDBSinkConfigBean);
		}
	}

	private static void createTableIfNotExists(SQLSinkConfigBean sqlSinkConfigBean, LinkedHashMap<String, AnvizentDataType> structure) throws Exception {
		boolean tableExists = SQLSinkService.checkIfTableExists(sqlSinkConfigBean, structure);
		if (!tableExists) {
			SQLSinkService.createTable(sqlSinkConfigBean, structure, Components.SQL_SINK.get(General.NAME));
		}
	}

	public static JobDetails getJobDetails(ConfigBean configBean, String internalRDDName) {
		JobDetails jobDetails = new JobDetails(ApplicationBean.getInstance().getSparkAppName(), configBean.getName(), configBean.getConfigName(),
		        internalRDDName);

		return jobDetails;
	}

	private static LinkedHashMap<String, AnvizentDataType> createEHStructure(LinkedHashMap<String, AnvizentDataType> structure) throws UnsupportedException {
		LinkedHashMap<String, AnvizentDataType> errorHandlerStructure = new LinkedHashMap<>();

		putJobStructure(errorHandlerStructure);
		putErrorStructure(errorHandlerStructure);
		putELTConstantsStructure(errorHandlerStructure);
		putErrorDataStructure(structure, errorHandlerStructure);

		return errorHandlerStructure;
	}

	private static void putELTConstantsStructure(LinkedHashMap<String, AnvizentDataType> errorHandlerStructure) throws UnsupportedException {
		errorHandlerStructure.put(ELTConstants.CREATED_BY, new AnvizentDataType(String.class));
		errorHandlerStructure.put(ELTConstants.CREATED_DATE, new AnvizentDataType(Date.class));
		errorHandlerStructure.put(ELTConstants.UPDATED_BY, new AnvizentDataType(String.class));
		errorHandlerStructure.put(ELTConstants.UPDATED_DATE, new AnvizentDataType(Date.class));
	}

	private static void putConstantsStructure(NoSQLConstantsConfigBean constantsConfigBean, LinkedHashMap<String, AnvizentDataType> errorHandlerStructure)
	        throws UnsupportedException {
		putRethinkDBConstants(constantsConfigBean.getFields(), constantsConfigBean.getTypes(), errorHandlerStructure);
		putRethinkDBConstants(constantsConfigBean.getLiteralFields(), constantsConfigBean.getLiteralTypes(), errorHandlerStructure);
	}

	private static void putRethinkDBConstants(ArrayList<String> fields, ArrayList<Class<?>> types,
	        LinkedHashMap<String, AnvizentDataType> errorHandlerStructure) throws UnsupportedException {
		if (fields != null && !fields.isEmpty()) {
			for (int i = 0; i < fields.size(); i++) {
				errorHandlerStructure.put(ELTConstants.EXTERNAL_DATA + fields.get(i), new AnvizentDataType(types.get(i)));
			}
		}
	}

	private static void putSQLConstants(ArrayList<String> fields, ArrayList<String> types, LinkedHashMap<String, AnvizentDataType> errorHandlerStructure)
	        throws UnsupportedException {
		if (fields != null && !fields.isEmpty()) {
			for (int i = 0; i < fields.size(); i++) {
				errorHandlerStructure.put(ELTConstants.EXTERNAL_DATA + fields.get(i), new AnvizentDataType(types.get(i).getClass()));
			}
		}
	}

	private static void putErrorDataStructure(LinkedHashMap<String, AnvizentDataType> structure,
	        LinkedHashMap<String, AnvizentDataType> errorHandlerStructure) {
		for (String key : structure.keySet()) {
			errorHandlerStructure.put(ELTConstants.ERROR_DATA + key, structure.get(key));
		}
	}

	private static void putErrorStructure(LinkedHashMap<String, AnvizentDataType> errorHandlerStructure) throws UnsupportedException {
		errorHandlerStructure.put(ELTConstants.ERROR_TYPE, new AnvizentDataType(String.class));
		errorHandlerStructure.put(ELTConstants.ERROR_MESSAGE, new AnvizentDataType(String.class));
		errorHandlerStructure.put(ELTConstants.ERROR_FULL_DETAILS, new AnvizentDataType(String.class));
	}

	private static void putJobStructure(LinkedHashMap<String, AnvizentDataType> errorHandlerStructure) throws UnsupportedException {
		errorHandlerStructure.put(ELTConstants.JOB_NAME, new AnvizentDataType(String.class));
		errorHandlerStructure.put(ELTConstants.COMPONENT, new AnvizentDataType(String.class));
		errorHandlerStructure.put(ELTConstants.COMPONENT_NAME, new AnvizentDataType(String.class));
		errorHandlerStructure.put(ELTConstants.INTERNAL_RDD_NAME, new AnvizentDataType(String.class));
	}

	public static void replaceWithComponentLevelConfigs(ErrorHandlerSink errorHandlerSink, LinkedHashMap<String, String> configs, SeekDetails seekDetails)
	        throws UnimplementedException, ImproperValidationException, InvalidConfigException {
		LinkedHashMap<String, String> ehConfigs = getSubConfigs(configs, General.EH);

		if (errorHandlerSink instanceof RethinkDBSinkConfigBean) {
			Validator validator = App.getErrorHandlerFactories().get(ErrorHandlers.EH_RETHINK_SINK).getValidator();
			validator.setExceptionDetails(seekDetails, ehConfigs);
			((RethinkDBSinkValidator) validator).replaceWithComponentSpecific(errorHandlerSink, ehConfigs);
		} else if (errorHandlerSink instanceof ArangoDBSinkConfigBean) {
			Validator validator = App.getErrorHandlerFactories().get(ErrorHandlers.EH_ARANGO_SINK).getValidator();
			validator.setExceptionDetails(seekDetails, ehConfigs);
			((ArangoDBSinkValidator) validator).replaceWithComponentSpecific(errorHandlerSink, ehConfigs);
		} else if (errorHandlerSink instanceof SQLSinkConfigBean) {
			Validator validator = App.getErrorHandlerFactories().get(ErrorHandlers.EH_SQL_SINK).getValidator();
			validator.setExceptionDetails(seekDetails, ehConfigs);
			((SQLSinkValidator) validator).replaceWithComponentSpecific(errorHandlerSink, ehConfigs);
		} else if (errorHandlerSink instanceof ConsoleSinkConfigBean) {
			Validator validator = App.getErrorHandlerFactories().get(ErrorHandlers.EH_CONSOLE_SINK).getValidator();
			validator.setExceptionDetails(seekDetails, ehConfigs);
			((ConsoleSinkValidator) validator).replaceWithComponentSpecific(errorHandlerSink, ehConfigs);
		} else {
			throw new UnimplementedException();
		}

	}

	private static LinkedHashMap<String, String> getSubConfigs(LinkedHashMap<String, String> configs, String prefix) throws ImproperValidationException {
		if (prefix == null) {
			throw new ImproperValidationException();
		}

		LinkedHashMap<String, String> subMap = new LinkedHashMap<>();

		for (String key : configs.keySet()) {
			if (StringUtils.startsWith(key, prefix + ".")) {
				subMap.put(key.substring((prefix + ".").length()), configs.get(key));
			} else {
				continue;
			}
		}

		return subMap;
	}
}
