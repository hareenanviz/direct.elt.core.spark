package com.anvizent.elt.core.spark.factory;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;
import com.anvizent.elt.core.lib.config.bean.RetriableConfigBean;
import com.anvizent.elt.core.lib.config.bean.RetryMandatoryConfigBean;
import com.anvizent.elt.core.lib.constant.Constants.StatsNames;
import com.anvizent.elt.core.lib.constant.StatsCategory;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.lib.stats.calculator.IStatsCalculator;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.listener.common.constant.StatsType;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.listener.common.store.ResourceConfig;
import com.anvizent.elt.core.listener.common.store.StatsSettingsStore;
import com.anvizent.elt.core.spark.config.ComponentConfiguration;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.constant.CounterType;
import com.anvizent.elt.core.spark.constant.Mapping;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidConfigurationReaderProperty;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.mapping.schema.validator.MappingSchemaValidator;
import com.anvizent.elt.core.spark.operation.factory.OperationFactory;
import com.anvizent.elt.core.spark.schema.validator.SchemaValidator;
import com.anvizent.elt.core.spark.sink.factory.SinkFactory;
import com.anvizent.elt.core.spark.source.factory.SourceFactory;
import com.anvizent.elt.core.spark.store.StatsStore;
import com.anvizent.elt.core.spark.validator.ResourceValidator;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public abstract class Factory implements Serializable {
	private static final long serialVersionUID = 1L;

	public ConfigAndMappingConfigBeans validateConfig(ComponentConfiguration componentConfiguration,
	        HashMap<String, ConfigAndMappingConfigBeans> configAndMappingConfigBeansMap, StatsSettingsStore statsSettingsStore, ResourceConfig resourceConfig)
	        throws InvalidConfigException, UnsupportedException, UnimplementedException, InvalidRelationException, InvalidConfigurationReaderProperty,
	        RecordProcessingException {
		Validator validator = getValidator();
		ConfigAndMappingConfigBeans configAndMappingConfigBeans = validator.validate(componentConfiguration, configAndMappingConfigBeansMap,
		        statsSettingsStore);

		if ((configAndMappingConfigBeans.getConfigBean() instanceof RetryMandatoryConfigBean && !(this instanceof RetryMandatoryFactory))
		        || (configAndMappingConfigBeans.getConfigBean() instanceof RetriableConfigBean && !(this instanceof RetriableFactory))
		        || (!(configAndMappingConfigBeans.getConfigBean() instanceof RetryMandatoryConfigBean) && this instanceof RetryMandatoryFactory)
		        || (!(configAndMappingConfigBeans.getConfigBean() instanceof RetriableConfigBean) && this instanceof RetriableFactory)) {
			throw new InvalidRelationException("Invalid relation between " + this.getClass() + " and " + configAndMappingConfigBeans.getConfigBean().getClass()
			        + ", one is retriable and once is not");
		}

		ResourceValidator resourceValidator = getResourceConfigValidator();
		if (resourceValidator != null) {
			resourceValidator.validate(configAndMappingConfigBeans.getConfigBean(), resourceConfig);
		}

		return configAndMappingConfigBeans;
	}

	protected Component mapping(ConfigAndMappingConfigBeans configAndMappingConfigBeans, StatsStore statsStore, Component component, String sourceStream,
	        StatsType globalStatsType)
	        throws ImproperValidationException, InvalidConfigException, UnsupportedException, InvalidRelationException, UnimplementedException, Exception {
		StatsType statsType = getStatsType(statsStore, globalStatsType);
		boolean componentLevelIn = false;
		boolean componentLevelOut = false;
		int numOfMappingConfigBeans = getNotEmptyMappingConfigBeans(configAndMappingConfigBeans.getMappingConfigBeans());
		int mappingsCount = 0;

		for (Mapping mapping : Mapping.values()) {
			ArrayList<String> streamNames = this instanceof SinkFactory ? new ArrayList<>(Arrays.asList(sourceStream))
			        : new ArrayList<>(component.getRdds().keySet());

			MappingConfigBean mappingConfigBean = configAndMappingConfigBeans.getMappingConfigBeans().get(mapping);
			if (mappingConfigBean != null) {
				mappingsCount++;
				if (mappingsCount == 1 && this instanceof SinkFactory) {
					componentLevelIn = true;
				}
			}

			if (mappingsCount == numOfMappingConfigBeans && (this instanceof SourceFactory || this instanceof OperationFactory)) {
				componentLevelOut = true;
			}

			mapping.getMappingFactory().createMappingAccumulators(mappingConfigBean, streamNames, componentLevelIn, componentLevelOut, statsType);
			mapping.getMappingFactory().createSpecialAccumulators(mappingConfigBean, streamNames, componentLevelIn, componentLevelOut, statsType);

			if (mappingConfigBean != null) {
				MappingSchemaValidator mappingSchemaValidator = mapping.getMappingFactory().getMappingSchemaValidator();
				if (mappingSchemaValidator != null) {
					InvalidConfigException invalidConfigException = new InvalidConfigException();
					invalidConfigException.setDetails(configAndMappingConfigBeans.getConfigBean());
					mappingSchemaValidator.validate(mappingConfigBean, component.getStructure(), invalidConfigException);
					if (invalidConfigException.getNumberOfExceptions() > 0) {
						throw invalidConfigException;
					}
				}
			}

			component = mapping.getMappingFactory().getComponent(configAndMappingConfigBeans.getConfigBean(), mappingConfigBean, component, streamNames,
			        configAndMappingConfigBeans.getErrorHandlerSink());
			componentLevelIn = false;
		}

		return component;
	}

	protected int getNotEmptyMappingConfigBeans(LinkedHashMap<Mapping, MappingConfigBean> mappingConfigBeans) {
		int count = 0;
		for (Mapping mapping : Mapping.values()) {
			if (mappingConfigBeans.get(mapping) != null) {
				count++;
			}
		}

		return count;
	}

	public void createAccumulators(ConfigAndMappingConfigBeans configAndMappingConfigBeans, StatsType globalStatsType) {
		StatsType statsType = getStatsType(configAndMappingConfigBeans.getStatsStore(), globalStatsType);

		if (statsType.equals(StatsType.NONE)) {
			return;
		}

		AnvizentAccumulator errorAnvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(),
		        configAndMappingConfigBeans.getConfigBean().getName(), getName(), StatsCategory.ERROR, StatsNames.ERROR, true,
		        getStatsCalculator(StatsCategory.ERROR, StatsNames.ERROR, getName()));
		ApplicationBean.getInstance().addAccumulator(configAndMappingConfigBeans.getConfigBean().getName(), getName(), errorAnvizentAccumulator);

		boolean componentLevel = isComponentLevel(configAndMappingConfigBeans.getMappingConfigBeans());
		createFactoryAccumulators(configAndMappingConfigBeans.getConfigBean(), statsType, componentLevel);
		createSpecialAccumulators(configAndMappingConfigBeans.getConfigBean(), statsType, componentLevel);
	}

	protected void createCustomAccumulators(ConfigAndMappingConfigBeans configAndMappingConfigBeans, StatsType statsType) throws InvalidConfigException {
		StatsStore statsStore = configAndMappingConfigBeans.getStatsStore();
		InvalidConfigException invalidConfigException = createInvalidConfigException(configAndMappingConfigBeans.getConfigBean());

		for (int i = 0; i < statsStore.getCalculatorClasses().size(); i++) {
			ArrayList<String> constructorArgs = statsStore.getConstructorArgs().get(i);
			ArrayList<String> constructorValues = statsStore.getConstructorValues().get(i);

			AnvizentAccumulator anvizentAccumulator = getCustomAnvizentAccumulator(configAndMappingConfigBeans.getConfigBean().getName(),
			        statsStore.getCalculatorClasses().get(i), constructorArgs, constructorValues, invalidConfigException);

			ApplicationBean.getInstance().addAccumulator(configAndMappingConfigBeans.getConfigBean().getName(), getName(), anvizentAccumulator);
		}

		// TODO componentLevel custom stats
	}

	@SuppressWarnings({ "unchecked" })
	private AnvizentAccumulator getCustomAnvizentAccumulator(String componentName, String calculatorClass, ArrayList<String> constructorArgs,
	        ArrayList<String> constructorValues, InvalidConfigException invalidConfigException) throws InvalidConfigException {
		AnvizentAccumulator anvizentAccumulator = null;
		try {
			Class<IStatsCalculator> statsCalculatorClass = (Class<IStatsCalculator>) Class.forName(calculatorClass);
			Constructor<IStatsCalculator> constructor = statsCalculatorClass
			        .getConstructor(new Class[] { StatsCategory.class, String.class }); /* take constructor types */

			IStatsCalculator statsCalculatorClassInstance = constructor.newInstance(StatsCategory.getInstance(constructorValues.get(0)),
			        constructorValues.get(1));

			Method statsCategoryMethod = statsCalculatorClass.getMethod("getStatsCategory", new Class[] {});
			Map<String, StatsCategory> statsCategory = (Map<String, StatsCategory>) statsCategoryMethod.invoke(statsCalculatorClassInstance, new Object[] {});
			Entry<String, StatsCategory> entry = statsCategory.entrySet().iterator().next();

			anvizentAccumulator = new AnvizentAccumulator(ApplicationBean.getInstance().getSparkSession(), componentName, getName(), entry.getValue(),
			        entry.getKey(), true, statsCalculatorClassInstance);

		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException | SecurityException | IllegalArgumentException
		        | InvocationTargetException exception) {
			invalidConfigException.add(exception.getMessage(), exception);
		}

		if (invalidConfigException.getNumberOfExceptions() > 0) {
			throw invalidConfigException;
		}

		return anvizentAccumulator;
	}

	private InvalidConfigException createInvalidConfigException(ConfigBean configBean) {
		InvalidConfigException invalidConfigException = new InvalidConfigException();
		invalidConfigException.setComponent(configBean.getName());
		invalidConfigException.setComponentName(configBean.getConfigName());
		invalidConfigException.setSeekDetails(configBean.getSeekDetails());

		return invalidConfigException;
	}

	protected boolean isComponentLevel(LinkedHashMap<Mapping, MappingConfigBean> mappingConfigBeans) {
		return getNotEmptyMappingConfigBeans(mappingConfigBeans) > 0 ? true : false;
	}

	protected StatsType getStatsType(StatsStore statsStore, StatsType globalStatsType) {
		return statsStore == null ? globalStatsType : statsStore.getStatsType();
	}

	protected boolean isCustomStatsCalculator(StatsStore statsStore) {
		return statsStore == null ? false : statsStore.getCalculatorClasses() != null ? true : false;
	}

	protected void count(CounterType counterType, Component component) {
		// TODO unimplemented
	}

	protected void count(CounterType counterType, ConfigBean configBean, LinkedHashMap<String, Component> component) {
		// TODO unimplemented
	}

	protected void count(ArrayList<CounterType> counterTypes, Component component) {
		// TODO unimplemented
	}

	public abstract String getName();

	public abstract DocHelper getDocHelper() throws InvalidParameter;

	public abstract Validator getValidator();

	public abstract ResourceValidator getResourceConfigValidator();

	public abstract SchemaValidator getSchemaValidator();

	public abstract Integer getMaxInputs();

	public abstract Integer getMinInputs();

	protected abstract IStatsCalculator getStatsCalculator(StatsCategory statsCategory, String statsName, String internalRDDName);

	protected abstract void createFactoryAccumulators(ConfigBean configBean, StatsType statsType, boolean componentLevel);

	protected abstract void createSpecialAccumulators(ConfigBean configBean, StatsType statsType, boolean componentLevel);
}
