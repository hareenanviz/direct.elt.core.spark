package com.anvizent.elt.core.spark;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLStreamHandler;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;

import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.lib.exception.InvalidSituationException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.listener.ApplicationListener;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.bean.ApplicationStatusDetails;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.listener.common.connection.ApplicationConnectionBean;
import com.anvizent.elt.core.listener.common.constant.ApplicationStatus;
import com.anvizent.elt.core.listener.common.constant.StatsType;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.listener.common.store.ErrorHandlerStore;
import com.anvizent.elt.core.listener.common.store.JobSettingStore;
import com.anvizent.elt.core.listener.common.store.ResourceConfig;
import com.anvizent.elt.core.listener.common.store.StatsSettingsStore;
import com.anvizent.elt.core.spark.common.util.ARGSValidationUtil;
import com.anvizent.elt.core.spark.config.ComponentConfiguration;
import com.anvizent.elt.core.spark.config.ConfigurationReader;
import com.anvizent.elt.core.spark.config.ConfigurationReaderBuilder;
import com.anvizent.elt.core.spark.config.ConfigurationReadingException;
import com.anvizent.elt.core.spark.config.DerivedComponentConfiguration;
import com.anvizent.elt.core.spark.config.DerivedComponentsReader;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.config.util.ProcessHTMLHelp;
import com.anvizent.elt.core.spark.constant.ConfigConstants;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General.ErrorHandlers;
import com.anvizent.elt.core.spark.constant.Constants;
import com.anvizent.elt.core.spark.constant.Constants.ARGSConstant;
import com.anvizent.elt.core.spark.constant.Constants.ExceptionMessage;
import com.anvizent.elt.core.spark.exception.ConfigReferenceNotFoundException;
import com.anvizent.elt.core.spark.exception.InvalidArgumentsException;
import com.anvizent.elt.core.spark.exception.InvalidConfigurationReaderProperty;
import com.anvizent.elt.core.spark.exception.InvalidInputForConfigException;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.filter.factory.FilterByExpressionFactory;
import com.anvizent.elt.core.spark.filter.factory.FilterByRegExFactory;
import com.anvizent.elt.core.spark.filter.factory.FilterByResultFactory;
import com.anvizent.elt.core.spark.operation.factory.ArangoDBFetcherFactory;
import com.anvizent.elt.core.spark.operation.factory.ArangoDBLookUpFactory;
import com.anvizent.elt.core.spark.operation.factory.EmptyFactory;
import com.anvizent.elt.core.spark.operation.factory.ExecuteSQLFactory;
import com.anvizent.elt.core.spark.operation.factory.ExpressionFactory;
import com.anvizent.elt.core.spark.operation.factory.GroupByFactory;
import com.anvizent.elt.core.spark.operation.factory.JoinFactory;
import com.anvizent.elt.core.spark.operation.factory.MLMLookupFactory;
import com.anvizent.elt.core.spark.operation.factory.OperationFactory;
import com.anvizent.elt.core.spark.operation.factory.RemoveDuplicatesByKeyFactory;
import com.anvizent.elt.core.spark.operation.factory.RepartitionFactory;
import com.anvizent.elt.core.spark.operation.factory.ResultFetcherFactory;
import com.anvizent.elt.core.spark.operation.factory.RethinkFetcherFactory;
import com.anvizent.elt.core.spark.operation.factory.RethinkLookUpFactory;
import com.anvizent.elt.core.spark.operation.factory.SQLFetcherFactory;
import com.anvizent.elt.core.spark.operation.factory.SQLLookUpFactory;
import com.anvizent.elt.core.spark.operation.factory.SequenceFactory;
import com.anvizent.elt.core.spark.operation.factory.UnionFactory;
import com.anvizent.elt.core.spark.reader.ErrorHandlerReader;
import com.anvizent.elt.core.spark.reader.JobSettingsReader;
import com.anvizent.elt.core.spark.reader.ResourceConfigReader;
import com.anvizent.elt.core.spark.reader.StatsSettingsReader;
import com.anvizent.elt.core.spark.sink.factory.ArangoDBSinkFactory;
import com.anvizent.elt.core.spark.sink.factory.CSVFileSinkFactory;
import com.anvizent.elt.core.spark.sink.factory.ConsoleSinkFactory;
import com.anvizent.elt.core.spark.sink.factory.JSONFileSinkFactory;
import com.anvizent.elt.core.spark.sink.factory.KafkaSinkFactory;
import com.anvizent.elt.core.spark.sink.factory.RethinkDBSinkFactory;
import com.anvizent.elt.core.spark.sink.factory.S3CSVFileSinkFactory;
import com.anvizent.elt.core.spark.sink.factory.S3JSONFileSinkFactory;
import com.anvizent.elt.core.spark.sink.factory.SQLSinkFactory;
import com.anvizent.elt.core.spark.sink.factory.SinkFactory;
import com.anvizent.elt.core.spark.source.factory.SourceArangoDBFactory;
import com.anvizent.elt.core.spark.source.factory.SourceCSVFileFactory;
import com.anvizent.elt.core.spark.source.factory.SourceFactory;
import com.anvizent.elt.core.spark.source.factory.SourceRethinkDBFactory;
import com.anvizent.elt.core.spark.source.factory.SourceS3CSVFileFactory;
import com.anvizent.elt.core.spark.source.factory.SourceSQLFactory;
import com.anvizent.util.ArgsUtil;
import com.univocity.parsers.common.DataProcessingException;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh <br />
 *         You can run this App with argha spark config as file or arguments
 *         <br />
 *         --sparkMaster: Use only for development environment not for any other
 *         environment. <br />
 *         --appName: Provide empty string to submit from command. <br />
 *         --derivedComponentConfigs: Derived component config properties file
 *         path or query<br />
 *         --configs: Config properties file path or query<br />
 *         --values: Values properties file path or query<br />
 *         --globalValues: Global values properties file path or query<br />
 *         --statsSettings: Stats Settings properties file path.<br />
 *         --errorHandlers: Error Handlers properties file path.<br />
 *         --jobSettings: Job Settings properties file path.<br />
 *         --jdbcUrl: For properties as queries.<br />
 *         --userName: For properties as queries.<br />
 *         --password: For properties as queries.<br />
 *         --driver: For properties as queries.<br />
 *         --resourceConfig: For resource grammar definitions, etc.<br />
 *         --privateKey: For security, if password is encrypted or queries have
 *         encryption.<br />
 *         --iv: For security, if password is encrypted or queries have
 *         encryption.<br />
 *         Use --help console to print all component usage<br />
 *         Use --help html to view all component usage in a html page. <br />
 *         --htmlFilePath: Html File Path to view all component usage in the
 *         html page. <br />
 *         --reProcess: true/false. Either to reprocess the html or show
 *         pre-processed html.<br />
 *
 */
public class App {

	private static final LinkedHashMap<String, Factory> FACTORIES = new LinkedHashMap<String, Factory>();
	private static final LinkedHashMap<String, Factory> ERROR_HANDLERS = new LinkedHashMap<String, Factory>();

	private App(String sparkMaster, String sparkAppName, ConfigurationReader configurationReader, StatsSettingsStore statsSettingsStore,
	        ErrorHandlerStore errorHandlerStore, JobSettingStore jobSettingsStore, ResourceConfig resourceConfig) throws InvalidConfigException, Exception {
		init();
		ArrayList<Factory> factories = new ArrayList<Factory>();
		ArrayList<ConfigAndMappingConfigBeans> configAndMappingConfigBeans = new ArrayList<>();
		HashMap<String, ConfigAndMappingConfigBeans> configAndMappingConfigBeansMap = new HashMap<>();
		HashMap<String, ArrayList<String>> configAndMappingConfigBeanSourcesMap = new HashMap<>();

		ApplicationBean applicationBeanInstance = ApplicationBean.getInstance();
		applicationBeanInstance.setSparkAppName(sparkAppName);
		applicationBeanInstance.setStatsSettingsStore(statsSettingsStore);
		applicationBeanInstance.setErrorHandlerStore(errorHandlerStore);
		applicationBeanInstance.setJobSettingStore(jobSettingsStore);
		applicationBeanInstance.setResourceConfig(resourceConfig);

		addFactoriesAndConfigs(configurationReader, factories, configAndMappingConfigBeans, configAndMappingConfigBeansMap,
		        configAndMappingConfigBeanSourcesMap, statsSettingsStore, resourceConfig);

		SparkSession sparkSession = createSparkSession(sparkMaster, sparkAppName);

		applicationBeanInstance.setSparkSession(sparkSession);

		int mb = 1024 * 1024;

		// Getting the runtime reference from system
		Runtime runtime = Runtime.getRuntime();

		System.out.println("##### Heap utilization statistics in driver [MB] #####");

		// Print used memory
		System.out.println("Used Memory:" + (runtime.totalMemory() - runtime.freeMemory()) / mb);

		// Print free memory
		System.out.println("Free Memory:" + runtime.freeMemory() / mb);

		// Print total available memory
		System.out.println("Total Memory:" + runtime.totalMemory() / mb);

		// Print Maximum available memory
		System.out.println("Max Memory:" + runtime.maxMemory() / mb);

		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory(sparkSession.sparkContext().hadoopConfiguration()) {
			@Override
			public URLStreamHandler createURLStreamHandler(String protocol) {
				if (protocol.equals("http") || protocol.equals("https")) {
					return null;
				}
				return super.createURLStreamHandler(protocol);
			}
		});

		process(factories, configAndMappingConfigBeans, statsSettingsStore);
	}

	private void init() {
		ApplicationBean.getInstance();
		ApplicationConnectionBean.getInstance();
	}

	private SparkSession createSparkSession(String sparkMaster, String sparkAppName) {
		Builder builder = SparkSession.builder().appName(sparkAppName).config("spark.yarn.maxAppAttempts", 1).config("spark.task.maxFailures", 1)
		        .config("spark.extraListeners", ApplicationListener.class.getName());

		if (sparkMaster != null) {
			builder = builder.master(sparkMaster);
		}

		return builder.getOrCreate();
	}

	private void process(ArrayList<Factory> factories, ArrayList<ConfigAndMappingConfigBeans> configAndMappingConfigBeans,
	        StatsSettingsStore statsSettingsStore) throws InvalidConfigException, Exception {
		LinkedHashMap<String, Component> components = ApplicationListener.getInstance().getComponents();

		for (int i = 0; i < factories.size(); i++) {
			Factory factory = factories.get(i);
			StatsType globalStatsType = null;

			if (statsSettingsStore != null) {
				globalStatsType = statsSettingsStore.getStatsType();
				factory.createAccumulators(configAndMappingConfigBeans.get(i), globalStatsType);
			}

			if (factory instanceof SourceFactory) {
				SourceFactory sourceFactory = (SourceFactory) factory;
				Component sourceComponent = sourceFactory.readAndProcess(configAndMappingConfigBeans.get(i), globalStatsType);
				components.put(sourceComponent.getName(), sourceComponent);
			} else if (factory instanceof OperationFactory) {
				OperationFactory operationFactory = (OperationFactory) factory;
				Component operationComponent = operationFactory.readAndProcess(configAndMappingConfigBeans.get(i), components, globalStatsType);
				components.put(operationComponent.getName(), operationComponent);
			} else if (factory instanceof SinkFactory) {
				SinkFactory sinkFactory = (SinkFactory) factory;
				sinkFactory.processAndWrite(configAndMappingConfigBeans.get(i), components, globalStatsType);
			} else {
				throw new InvalidSituationException();
			}
		}
	}

	private static void iniFactories() {
		initSourceFactories();
		initOperationFactories();
		initFilterFactories();
		initSinkFactories();
	}

	private static LinkedHashMap<String, LinkedHashMap<String, Factory>> getFactoriesForDoc() {
		LinkedHashMap<String, LinkedHashMap<String, Factory>> categorisedFactories = new LinkedHashMap<String, LinkedHashMap<String, Factory>>();
		LinkedHashMap<String, Factory> sourceFactories = new LinkedHashMap<String, Factory>();
		LinkedHashMap<String, Factory> operationFactories = new LinkedHashMap<String, Factory>();
		LinkedHashMap<String, Factory> filterFactories = new LinkedHashMap<String, Factory>();
		LinkedHashMap<String, Factory> sinkFactories = new LinkedHashMap<String, Factory>();

		categorisedFactories.put(General.SOURCE, sourceFactories);
		categorisedFactories.put(General.OPERATION, operationFactories);
		categorisedFactories.put(General.FILTER, filterFactories);
		categorisedFactories.put(General.SINK, sinkFactories);

		initSourceFactoriesForDoc(sourceFactories);
		initOperationFactoriesForDoc(operationFactories);
		initFilterFactoriesForDoc(filterFactories);
		initSinkFactoriesForDoc(sinkFactories);

		return categorisedFactories;
	}

	private static void initSourceFactories() {
		FACTORIES.put(ConfigConstants.Source.Components.SOURCE_SQL.get(General.CONFIG), new SourceSQLFactory());
		FACTORIES.put(ConfigConstants.Source.Components.SOURCE_CSV.get(General.CONFIG), new SourceCSVFileFactory());
		FACTORIES.put(ConfigConstants.Source.Components.SOURCE_S3_CSV.get(General.CONFIG), new SourceS3CSVFileFactory());
		FACTORIES.put(ConfigConstants.Source.Components.SOURCE_RETHINKDB.get(General.CONFIG), new SourceRethinkDBFactory());
		FACTORIES.put(ConfigConstants.Source.Components.SOURCE_ARANGODB.get(General.CONFIG), new SourceArangoDBFactory());
	}

	private static void initSourceFactoriesForDoc(LinkedHashMap<String, Factory> sourceFactories) {
		sourceFactories.put(ConfigConstants.Source.Components.SOURCE_SQL.get(General.CONFIG), new SourceSQLFactory());
		sourceFactories.put(ConfigConstants.Source.Components.SOURCE_CSV.get(General.CONFIG), new SourceCSVFileFactory());
		sourceFactories.put(ConfigConstants.Source.Components.SOURCE_S3_CSV.get(General.CONFIG), new SourceS3CSVFileFactory());
		sourceFactories.put(ConfigConstants.Source.Components.SOURCE_RETHINKDB.get(General.CONFIG), new SourceRethinkDBFactory());
		sourceFactories.put(ConfigConstants.Source.Components.SOURCE_ARANGODB.get(General.CONFIG), new SourceArangoDBFactory());
	}

	private static void initOperationFactories() {
		FACTORIES.put(ConfigConstants.Operation.Components.SQL_LOOKUP.get(General.CONFIG), new SQLLookUpFactory());
		FACTORIES.put(ConfigConstants.Operation.Components.GROUP_BY.get(General.CONFIG), new GroupByFactory());
		FACTORIES.put(ConfigConstants.Operation.Components.JOIN.get(General.CONFIG), new JoinFactory());
		FACTORIES.put(ConfigConstants.Operation.Components.EXPRESSION.get(General.CONFIG), new ExpressionFactory());
		FACTORIES.put(ConfigConstants.Operation.Components.EXECUTE_SQL.get(General.CONFIG), new ExecuteSQLFactory());
		FACTORIES.put(ConfigConstants.Operation.Components.SEQUENCE.get(General.CONFIG), new SequenceFactory());
		FACTORIES.put(ConfigConstants.Operation.Components.EMPTY.get(General.CONFIG), new EmptyFactory());
		FACTORIES.put(ConfigConstants.Operation.Components.UNION.get(General.CONFIG), new UnionFactory());
		FACTORIES.put(ConfigConstants.Operation.Components.SQL_FETCHER.get(General.CONFIG), new SQLFetcherFactory());
		FACTORIES.put(ConfigConstants.Operation.Components.REMOVE_DUPLICATES_BY_KEY.get(General.CONFIG), new RemoveDuplicatesByKeyFactory());
		FACTORIES.put(ConfigConstants.Operation.Components.REPARTITION.get(General.CONFIG), new RepartitionFactory());
		FACTORIES.put(ConfigConstants.Operation.Components.RETHINK_LOOKUP.get(General.CONFIG), new RethinkLookUpFactory());
		FACTORIES.put(ConfigConstants.Operation.Components.RETHINK_FETCHER.get(General.CONFIG), new RethinkFetcherFactory());
		FACTORIES.put(ConfigConstants.Operation.Components.MLM_LOOKUP.get(General.CONFIG), new MLMLookupFactory());
		FACTORIES.put(ConfigConstants.Operation.Components.ARANGO_DB_LOOKUP.get(General.CONFIG), new ArangoDBLookUpFactory());
		FACTORIES.put(ConfigConstants.Operation.Components.ARANGO_DB_FETCHER.get(General.CONFIG), new ArangoDBFetcherFactory());
		FACTORIES.put(ConfigConstants.Operation.Components.RESULT_FETCHER.get(General.CONFIG), new ResultFetcherFactory());
	}

	private static void initOperationFactoriesForDoc(LinkedHashMap<String, Factory> operationFactories) {
		operationFactories.put(ConfigConstants.Operation.Components.SQL_LOOKUP.get(General.CONFIG), new SQLLookUpFactory());
		operationFactories.put(ConfigConstants.Operation.Components.GROUP_BY.get(General.CONFIG), new GroupByFactory());
		operationFactories.put(ConfigConstants.Operation.Components.JOIN.get(General.CONFIG), new JoinFactory());
		operationFactories.put(ConfigConstants.Operation.Components.EXPRESSION.get(General.CONFIG), new ExpressionFactory());
		operationFactories.put(ConfigConstants.Operation.Components.EXECUTE_SQL.get(General.CONFIG), new ExecuteSQLFactory());
		operationFactories.put(ConfigConstants.Operation.Components.SEQUENCE.get(General.CONFIG), new SequenceFactory());
		operationFactories.put(ConfigConstants.Operation.Components.EMPTY.get(General.CONFIG), new EmptyFactory());
		operationFactories.put(ConfigConstants.Operation.Components.UNION.get(General.CONFIG), new UnionFactory());
		operationFactories.put(ConfigConstants.Operation.Components.SQL_FETCHER.get(General.CONFIG), new SQLFetcherFactory());
		operationFactories.put(ConfigConstants.Operation.Components.REMOVE_DUPLICATES_BY_KEY.get(General.CONFIG), new RemoveDuplicatesByKeyFactory());
		operationFactories.put(ConfigConstants.Operation.Components.REPARTITION.get(General.CONFIG), new RepartitionFactory());
		operationFactories.put(ConfigConstants.Operation.Components.RETHINK_LOOKUP.get(General.CONFIG), new RethinkLookUpFactory());
		operationFactories.put(ConfigConstants.Operation.Components.RETHINK_FETCHER.get(General.CONFIG), new RethinkFetcherFactory());
		operationFactories.put(ConfigConstants.Operation.Components.MLM_LOOKUP.get(General.CONFIG), new MLMLookupFactory());
		operationFactories.put(ConfigConstants.Operation.Components.ARANGO_DB_LOOKUP.get(General.CONFIG), new ArangoDBLookUpFactory());
		operationFactories.put(ConfigConstants.Operation.Components.RESULT_FETCHER.get(General.CONFIG), new ResultFetcherFactory());
	}

	private static void initFilterFactories() {
		FACTORIES.put(ConfigConstants.Filter.Components.FILTER_BY_EXPRESSION.get(General.CONFIG), new FilterByExpressionFactory());
		FACTORIES.put(ConfigConstants.Filter.Components.FILTER_BY_REGULAR_EXPRESSION.get(General.CONFIG), new FilterByRegExFactory());
		FACTORIES.put(ConfigConstants.Filter.Components.FILTER_BY_RESULT.get(General.CONFIG), new FilterByResultFactory());
	}

	private static void initFilterFactoriesForDoc(LinkedHashMap<String, Factory> filterFactories) {
		filterFactories.put(ConfigConstants.Filter.Components.FILTER_BY_EXPRESSION.get(General.CONFIG), new FilterByExpressionFactory());
		filterFactories.put(ConfigConstants.Filter.Components.FILTER_BY_REGULAR_EXPRESSION.get(General.CONFIG), new FilterByRegExFactory());
		filterFactories.put(ConfigConstants.Filter.Components.FILTER_BY_RESULT.get(General.CONFIG), new FilterByResultFactory());
	}

	private static void initSinkFactories() {
		FACTORIES.put(ConfigConstants.Sink.Components.JSON_SINK.get(General.CONFIG), new JSONFileSinkFactory());
		FACTORIES.put(ConfigConstants.Sink.Components.S3_JSON_SINK.get(General.CONFIG), new S3JSONFileSinkFactory());
		FACTORIES.put(ConfigConstants.Sink.Components.SQL_SINK.get(General.CONFIG), new SQLSinkFactory());
		FACTORIES.put(ConfigConstants.Sink.Components.RETHINK_DB_SINK.get(General.CONFIG), new RethinkDBSinkFactory());
		FACTORIES.put(ConfigConstants.Sink.Components.CONSOLE_SINK.get(General.CONFIG), new ConsoleSinkFactory());
		FACTORIES.put(ConfigConstants.Sink.Components.CSV_SINK.get(General.CONFIG), new CSVFileSinkFactory());
		FACTORIES.put(ConfigConstants.Sink.Components.S3_CSV_SINK.get(General.CONFIG), new S3CSVFileSinkFactory());
		FACTORIES.put(ConfigConstants.Sink.Components.KAFKA_SINK.get(General.CONFIG), new KafkaSinkFactory());
		FACTORIES.put(ConfigConstants.Sink.Components.ARANGO_DB_SINK.get(General.CONFIG), new ArangoDBSinkFactory());
	}

	private static void initSinkFactoriesForDoc(LinkedHashMap<String, Factory> sinkFactories) {
		sinkFactories.put(ConfigConstants.Sink.Components.JSON_SINK.get(General.CONFIG), new JSONFileSinkFactory());
		sinkFactories.put(ConfigConstants.Sink.Components.S3_JSON_SINK.get(General.CONFIG), new S3JSONFileSinkFactory());
		sinkFactories.put(ConfigConstants.Sink.Components.SQL_SINK.get(General.CONFIG), new SQLSinkFactory());
		sinkFactories.put(ConfigConstants.Sink.Components.RETHINK_DB_SINK.get(General.CONFIG), new RethinkDBSinkFactory());
		sinkFactories.put(ConfigConstants.Sink.Components.CONSOLE_SINK.get(General.CONFIG), new ConsoleSinkFactory());
		sinkFactories.put(ConfigConstants.Sink.Components.CSV_SINK.get(General.CONFIG), new CSVFileSinkFactory());
		sinkFactories.put(ConfigConstants.Sink.Components.S3_CSV_SINK.get(General.CONFIG), new S3CSVFileSinkFactory());
		sinkFactories.put(ConfigConstants.Sink.Components.KAFKA_SINK.get(General.CONFIG), new KafkaSinkFactory());
		sinkFactories.put(ConfigConstants.Sink.Components.ARANGO_DB_SINK.get(General.CONFIG), new ArangoDBSinkFactory());
	}

	/**
	 * @param args <br />
	 *             --sparkMaster: Use only for development environment not for any
	 *             other environment. <br />
	 *             --appName: Provide empty string to submit from command. <br />
	 *             --derivedComponentConfigs: Derived component config properties
	 *             file path or query<br />
	 *             --configs: Config properties file path or query<br />
	 *             --values: Values properties file path or query<br />
	 *             --globalValues: Global values properties file path or query
	 *             <br />
	 *             --statsSettings: Stats Settings properties file path.<br />
	 *             --errorHandlers: Error Handlers properties file path.<br />
	 *             --jobSettings: Job Settings properties file path.<br />
	 *             --jdbcUrl: For properties as queries.<br />
	 *             --userName: For properties as queries.<br />
	 *             --password: For properties as queries.<br />
	 *             --driver: For properties as queries.<br />
	 *             --privateKey: For security, if password is encrypted or queries
	 *             have encryption.<br />
	 *             --iv: For security, if password is encrypted or queries have
	 *             encryption.<br />
	 *             Use --help console to print all component usage<br />
	 *             Use --help html to view all component usage in a html page.
	 *             <br />
	 *             --htmlFilePath: Html File Path to view all component usage in the
	 *             html page. <br />
	 *             --reProcess: true/false. Either to reprocess the html or show
	 *             pre-processed html.<br />
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		JobSettingStore jobSettingsStore = null;
		try {
			printARGS(args);

			LinkedHashMap<String, ArrayList<String>> arguments = ArgsUtil.contextArgsToListMap(args);

			if (!validateARGS(arguments)) {
				return;
			}

			iniFactories();
			initErrorHandlers();

			StatsSettingsStore statsSettingsStore = StatsSettingsReader.getStatsSettingsStore(arguments);
			ErrorHandlerStore errorHandlerStore = ErrorHandlerReader.getErrorHandlerStore(arguments, ERROR_HANDLERS);
			jobSettingsStore = JobSettingsReader.getJobSettingsStore(arguments);
			ResourceConfig resourceConfig = new ResourceConfigReader().getJobConfig(arguments);

			LinkedHashMap<String, DerivedComponentConfiguration> derivedComponents = new DerivedComponentsReader(arguments, FACTORIES.keySet())
			        .getDerivedComponents();

			String sparkMaster = (arguments.get(ARGSConstant.SPARK_MASTER) == null || arguments.get(ARGSConstant.SPARK_MASTER).isEmpty()
			        || arguments.get(ARGSConstant.SPARK_MASTER).get(0) == null || arguments.get(ARGSConstant.SPARK_MASTER).get(0).isEmpty()) ? null
			                : arguments.get(ARGSConstant.SPARK_MASTER).get(0);

			new App(sparkMaster, arguments.get(ARGSConstant.APP_NAME).get(0),
			        new ConfigurationReaderBuilder(arguments, derivedComponents, arguments.get(ARGSConstant.CONFIGS).get(0), ConfigConstants.General.EOC)
			                .build(),
			        statsSettingsStore, errorHandlerStore, jobSettingsStore, resourceConfig);
		} catch (Exception exception) {
			exception.printStackTrace();
			System.out.println("ApplicationBean.getInstance().getJobSettingStore(): " + ApplicationBean.getInstance().getJobSettingStore());
			ApplicationConnectionBean.getInstance().closeAll();

			if (jobSettingsStore != null) {
				ApplicationBean.getInstance().setJobSettingStore(jobSettingsStore);
				ApplicationStatusDetails applicationStatusDetails = getApplicationStatusDetails(exception);
				System.out.println("main: applicationStatusDetails: " + applicationStatusDetails);
				ApplicationBean.getInstance().storeApplicationEndTime(applicationStatusDetails);
				ApplicationBean.getInstance().setSentToInitiator(true);
			}

			throw exception;
		}
	}

	private static ApplicationStatusDetails getApplicationStatusDetails(Exception exception) {
		System.out.println("getApplicationStatusDetails");
		ApplicationStatusDetails applicationStatusDetails = new ApplicationStatusDetails();

		if (exception instanceof RecordProcessingException) {
			applicationStatusDetails.setApplicationStatus(ApplicationStatus.RECORD_PROCESSING_FAILED);
		} else if (exception instanceof DataProcessingException) {
			applicationStatusDetails.setApplicationStatus(ApplicationStatus.DATA_CORRUPTED);
		} else if (exception instanceof InvalidConfigException) {
			applicationStatusDetails.setApplicationStatus(ApplicationStatus.VALIDATION_FAILED);
		} else {
			applicationStatusDetails.setApplicationStatus(ApplicationStatus.FAILED);
		}

		applicationStatusDetails.setMessage(getExceptionMessage(exception));

		return applicationStatusDetails;
	}

	private static String getExceptionMessage(Throwable exception) {
		String message = "";

		do {
			System.out.println("getExceptionMessage: exception.getMessage(): " + exception.getMessage());
			System.out.println("getExceptionMessage: exception.getCause(): " + exception.getCause());
			System.out.println("getExceptionMessage: message: " + message);
			if (exception != null) {
				if (exception.getMessage() != null) {
					message += (message.isEmpty() ? "" : "\nCause: ") + exception.getMessage();
				}

				exception = exception.getCause();
			}
		} while (exception != null && exception.getCause() != null);

		return message;
	}

	private static void printARGS(String[] args) {
		for (int i = 0; i < args.length; i++) {
			System.out.println("arg" + i + ": " + args[i]);
		}
	}

	private static void initErrorHandlers() {
		ERROR_HANDLERS.put(ErrorHandlers.EH_RETHINK_SINK, FACTORIES.get(ConfigConstants.Sink.Components.RETHINK_DB_SINK.get(General.CONFIG)));
		ERROR_HANDLERS.put(ErrorHandlers.EH_ARANGO_SINK, FACTORIES.get(ConfigConstants.Sink.Components.ARANGO_DB_SINK.get(General.CONFIG)));
		ERROR_HANDLERS.put(ErrorHandlers.EH_SQL_SINK, FACTORIES.get(ConfigConstants.Sink.Components.SQL_SINK.get(General.CONFIG)));
		ERROR_HANDLERS.put(ErrorHandlers.EH_CONSOLE_SINK, FACTORIES.get(ConfigConstants.Sink.Components.CONSOLE_SINK.get(General.CONFIG)));
	}

	private static boolean validateARGS(LinkedHashMap<String, ArrayList<String>> arguments) throws Exception {
		if (arguments == null || arguments.isEmpty() || arguments.size() < 1) {
			onErrorPrintUsage(new InvalidArgumentsException(ExceptionMessage.INVALID_NUMBER_OF_ARGUMENT));
		} else {
			InvalidArgumentsException exception = new InvalidArgumentsException();

			Boolean helpReturnValue = processHelp(arguments, exception);
			if (helpReturnValue == null) {
				validateARGS(arguments, exception, helpReturnValue);
			} else {
				return helpReturnValue;
			}
		}

		return true;
	}

	private static void validateARGS(LinkedHashMap<String, ArrayList<String>> arguments, InvalidArgumentsException exception, Boolean helpReturnValue)
	        throws Exception {

		ARGSValidationUtil.validateMandatoryARGS(arguments, exception);
		onErrorPrintUsage(exception);
	}

	private static Boolean processHelp(LinkedHashMap<String, ArrayList<String>> arguments, InvalidArgumentsException exception)
	        throws InvalidArgumentsException, IOException, InvalidParameter, InterruptedException {
		String helpValue = ARGSValidationUtil.getHelpValue(arguments, exception);

		onErrorPrintUsage(exception);

		if (helpValue != null) {
			if (exception.getNumberOfExceptions() == 0) {
				return process(arguments, helpValue);
			} else {
				return true;
			}
		} else {
			return null;
		}
	}

	private static void onErrorPrintUsage(InvalidArgumentsException exception) throws InvalidArgumentsException, InterruptedException {
		if (exception.getNumberOfExceptions() > 0) {
			printUsage();
			Thread.sleep(1000);
			throw exception;
		}
	}

	private static boolean process(LinkedHashMap<String, ArrayList<String>> arguments, String helpValue) throws IOException, InvalidParameter {
		if (helpValue.equals(ARGSConstant.HTML)) {
			return process(arguments);
		} else {
			getFactoriesForDoc();
			return false;
		}
	}

	private static boolean process(LinkedHashMap<String, ArrayList<String>> arguments) throws IOException, InvalidParameter {
		ProcessHTMLHelp.process(getFactoriesForDoc(),
		        (arguments.get(ARGSConstant.HTML_FILE_PATH) == null || arguments.get(ARGSConstant.HTML_FILE_PATH).isEmpty()
		                || arguments.get(ARGSConstant.HTML_FILE_PATH).get(0) == null || arguments.get(ARGSConstant.HTML_FILE_PATH).get(0).isEmpty()
		                        ? Constants.CURRENT_DIRECTORY
		                        : new File(arguments.get(ARGSConstant.HTML_FILE_PATH).get(0))),
		        arguments.get(ARGSConstant.REPROCESS) == null || arguments.get(ARGSConstant.REPROCESS).isEmpty()
		                || arguments.get(ARGSConstant.REPROCESS).get(0) == null || arguments.get(ARGSConstant.REPROCESS).get(0).isEmpty() ? false
		                        : Boolean.parseBoolean(arguments.get(ARGSConstant.REPROCESS).get(0)));
		return false;
	}

	private void addFactoriesAndConfigs(ConfigurationReader configurationReader, ArrayList<Factory> factories,
	        ArrayList<ConfigAndMappingConfigBeans> configAndMappingConfigBeans, HashMap<String, ConfigAndMappingConfigBeans> configAndMappingConfigBeansMap,
	        HashMap<String, ArrayList<String>> configAndMappingConfigBeanSourcesMap, StatsSettingsStore statsSettingsStore, ResourceConfig resourceConfig)
	        throws InvalidInputForConfigException, IOException, InvalidConfigException, ConfigReferenceNotFoundException, UnsupportedException,
	        ConfigurationReadingException, UnimplementedException, InvalidRelationException, InvalidConfigurationReaderProperty, RecordProcessingException {
		ComponentConfiguration componentConfiguration = null;

		while ((componentConfiguration = configurationReader.getNextConfiguration()) != null) {
			addFactoryAndConfig(factories, configAndMappingConfigBeans, configAndMappingConfigBeansMap, configAndMappingConfigBeanSourcesMap,
			        componentConfiguration, configurationReader, statsSettingsStore, resourceConfig);
		}
	}

	private void addFactoryAndConfig(ArrayList<Factory> factories, ArrayList<ConfigAndMappingConfigBeans> configAndMappingConfigBeans,
	        HashMap<String, ConfigAndMappingConfigBeans> configAndMappingConfigBeansMap,
	        HashMap<String, ArrayList<String>> configAndMappingConfigBeanSourcesMap, ComponentConfiguration componentConfiguration,
	        ConfigurationReader configurationReader, StatsSettingsStore statsSettingsStore, ResourceConfig resourceConfig)
	        throws InvalidInputForConfigException, IOException, InvalidConfigException, ConfigReferenceNotFoundException, UnsupportedException,
	        UnimplementedException, InvalidRelationException, InvalidConfigurationReaderProperty, RecordProcessingException {
		Factory factory = FACTORIES.get(componentConfiguration.getConfigurationName());

		if (factory == null) {
			throw new InvalidInputForConfigException(
			        MessageFormat.format(ExceptionMessage.INVALID_CONFIGURATION_KEY, componentConfiguration.getConfigurationName()),
			        configurationReader.getSeekName(), configurationReader.getSeek());
		}

		if (factories.size() == 0 && !(factory instanceof SourceFactory)) {
			throw new InvalidInputForConfigException(ExceptionMessage.FIRST_CONFIGURATION_SHOULD_BE_SOURCE, configurationReader.getSeekName(),
			        configurationReader.getSeek());
		}

		factories.add(factory);
		ConfigAndMappingConfigBeans configAndMappingConfigBean = factory.validateConfig(componentConfiguration, configAndMappingConfigBeansMap,
		        statsSettingsStore, resourceConfig);

		configAndMappingConfigBeans.add(configAndMappingConfigBean);
		configAndMappingConfigBeansMap.put(configAndMappingConfigBean.getConfigBean().getName(), configAndMappingConfigBean);
		if (configAndMappingConfigBean.getConfigBean().getSources() != null && !configAndMappingConfigBean.getConfigBean().getSources().isEmpty()) {
			for (String source : configAndMappingConfigBean.getConfigBean().getSources()) {
				if (!configAndMappingConfigBeanSourcesMap.containsKey(source)) {
					configAndMappingConfigBeanSourcesMap.put(source, new ArrayList<>());
				} else {
					configAndMappingConfigBean.getConfigBean().setMultiOut(true);
				}

				configAndMappingConfigBeanSourcesMap.get(source).add(configAndMappingConfigBean.getConfigBean().getName());
			}
		}
	}

	public static LinkedHashMap<String, Factory> getErrorHandlerFactories() {
		// TODO
		return ERROR_HANDLERS;
	}

	private static void printUsage() {
		System.out.println("-- Usage --");
		System.out.println("--" + ARGSConstant.SPARK_MASTER + ": Use only for development environment not for any other environment.");
		System.out.println("--" + ARGSConstant.APP_NAME + ": Provide empty string to submit from command.");
		System.out.println("--" + ARGSConstant.DERIVED_COMPONENT_CONFIGS + ": Derived component config properties file path or query");
		System.out.println("--" + ARGSConstant.CONFIGS + ": Config properties file path or query");
		System.out.println("--" + ARGSConstant.VALUES + ": Values properties file path or query");
		System.out.println("--" + ARGSConstant.GLOBAL_VALUES + ": Global values properties file path or query");
		System.out.println("--" + ARGSConstant.STATS_SETTINGS + ": Stats Settings properties file path or query");
		System.out.println("--" + ARGSConstant.ERROR_HANDLERS + ": Error Handlers properties file path or query.");
		System.out.println("--" + ARGSConstant.JOB_SETTINGS + ": Job Settings properties file path or query.");
		System.out.println("--" + ARGSConstant.RESOURCE_CONFIG + ": Job config properties file path or query.");
		System.out.println("--" + ARGSConstant.JDBC_URL + ": For properties as queries.");
		System.out.println("--" + ARGSConstant.USER_NAME + ": For properties as queries.");
		System.out.println("--" + ARGSConstant.PASSWORD + ": For properties as queries.");
		System.out.println("--" + ARGSConstant.DRIVER + ": For properties as queries. All 4 jdbc details required for queries.");
		System.out.println("--" + ARGSConstant.PRIVATE_KEY + ": For security, if password is encrypted or queries have encryption.");
		System.out.println(
		        "--" + ARGSConstant.IV + ": For security, if password is encrypted or queries have encryption. Both privateKey & iv required for encryption.");
		System.out.println("Use --" + ARGSConstant.HELP + " " + ARGSConstant.CONSOLE + ": to print all component usage");
		System.out.println("Use --" + ARGSConstant.HELP + " " + ARGSConstant.HTML + ": to view all component usage in a html page.");
		System.out.println("--" + ARGSConstant.HTML_FILE_PATH + ": Html File Path to view all component usage in the html page.");
		System.out.println("--" + ARGSConstant.REPROCESS + ": true/false. Either to reprocess the html or show pre-processed html.");
	}
}
