package com.anvizent.elt.core.spark.validator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.commons.lang3.StringUtils;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;
import com.anvizent.elt.core.lib.config.bean.RetriableConfigBean;
import com.anvizent.elt.core.lib.config.bean.RetryMandatoryConfigBean;
import com.anvizent.elt.core.lib.config.bean.SeekDetails;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.constant.StatsType;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.listener.common.store.StatsSettingsStore;
import com.anvizent.elt.core.spark.common.util.ErrorHandlerUtil;
import com.anvizent.elt.core.spark.config.ComponentConfiguration;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General.ErrorHandlers;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General.StatsSettings;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant.Message;
import com.anvizent.elt.core.spark.constant.Mapping;
import com.anvizent.elt.core.spark.exception.StatsSettingsException;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.operation.factory.EmptyFactory;
import com.anvizent.elt.core.spark.sink.config.bean.SinkConfigBean;
import com.anvizent.elt.core.spark.source.factory.SourceFactory;
import com.anvizent.elt.core.spark.store.StatsStore;

/**
 * @author Hareen Bejjanki
 *
 */
public abstract class Validator implements Serializable {
	private static final long serialVersionUID = 1L;

	private Factory factory;
	protected InvalidConfigException exception;

	public Validator(Factory factory) {
		this.factory = factory;
		this.exception = new InvalidConfigException();
	}

	public ConfigAndMappingConfigBeans validate(ComponentConfiguration componentConfiguration,
	        HashMap<String, ConfigAndMappingConfigBeans> configAndMappingConfigBeansMap, StatsSettingsStore statsSettingsStore)
	        throws InvalidConfigException, ImproperValidationException, UnsupportedException, UnimplementedException {
		LinkedHashMap<String, String> configs = componentConfiguration.getConfiguration();
		setExceptionDetails(componentConfiguration.getSeekDetails(), configs);
		validateComponentStatsSettings(configs, statsSettingsStore);

		validateMandatoryField(configs, General.NAME);
		ArrayList<String> sources = ConfigUtil.getArrayList(configs, General.SOURCE, exception);
		validateNameAndSource(sources, configs, configAndMappingConfigBeansMap);

		ConfigAndMappingConfigBeans configAndMappingConfigBeans = new ConfigAndMappingConfigBeans();
		configAndMappingConfigBeans.setStatsStore(validateAndGetStatsStore(configs));
		configAndMappingConfigBeans.setErrorHandlerSink(validateAndGetErrorHandlerStore(configs, componentConfiguration.getSeekDetails()));
		configAndMappingConfigBeans.setConfigBean(validateFactoryConfig(configs, configAndMappingConfigBeans));
		configAndMappingConfigBeans.setMappingConfigBeans(validateAndGetMappingConfigBeans(configs, componentConfiguration.getSeekDetails()));

		validateAndSetOther(configs, configAndMappingConfigBeans.getConfigBean());

		if (factory instanceof EmptyFactory && isAllEmpty(configAndMappingConfigBeans.getMappingConfigBeans())) {
			exception.add("Provide atleast one mapping in empty component.");
		}

		if (exception.getNumberOfExceptions() > 0) {
			throw exception;
		} else {
			setConfigDetails(configAndMappingConfigBeans, configs, sources, componentConfiguration);

			return configAndMappingConfigBeans;
		}
	}

	private ErrorHandlerSink validateAndGetErrorHandlerStore(LinkedHashMap<String, String> configs, SeekDetails seekDetails)
	        throws ImproperValidationException, UnimplementedException, InvalidConfigException {
		if (configs.containsKey(General.SOEH) && configs.containsKey(General.EOEH)) {
			if (configs.get(ErrorHandlers.EH_NAME) == null) {
				exception.add(Message.CAN_NOT_BE_EMPTY, ErrorHandlers.EH_NAME);
			} else if (ApplicationBean.getInstance().getErrorHandlerStore() == null) {
				exception.add(Message.INVALID_TO_SPECIFY_SOEH_AND_EOEH);
			} else if (!ApplicationBean.getInstance().getErrorHandlerStore().getEHSinks().containsKey(configs.get(ErrorHandlers.EH_NAME))) {
				exception.add(Message.DEFAULT_ERROR_HANDLER_NOT_FOUND_WITH_NAME, configs.get(ErrorHandlers.EH_NAME));
			} else {
				ErrorHandlerSink errorHandlerSink = ApplicationBean.getInstance().getErrorHandlerStore().getEHSinks().get(configs.get(ErrorHandlers.EH_NAME));
				validateAndSetOther(configs, (ConfigBean) errorHandlerSink);
				ErrorHandlerUtil.replaceWithComponentLevelConfigs(errorHandlerSink, configs, seekDetails);

				return errorHandlerSink;
			}

			return null;
		} else if (ApplicationBean.getInstance().getErrorHandlerStore() != null) {
			ErrorHandlerSink errorHandlerSink = ApplicationBean.getInstance().getErrorHandlerStore().getEHSinks()
			        .get(ApplicationBean.getInstance().getErrorHandlerStore().getDefaultEHResourceName());
			validateAndSetOther(configs, (ConfigBean) errorHandlerSink);
			ErrorHandlerUtil.replaceWithComponentLevelConfigs(errorHandlerSink, configs, seekDetails);

			return errorHandlerSink;
		} else {
			return null;
		}
	}

	private void validateAndSetOther(LinkedHashMap<String, String> configs, ConfigBean configBean) {
		Integer maxRetryCount = ConfigUtil.getInteger(configs, General.MAX_RETRY_COUNT, exception);
		Long retryDelay = ConfigUtil.getLong(configs, General.RETRY_DELAY, exception);

		if (configBean instanceof RetriableConfigBean) {
			configBean.setMaxRetryCount(maxRetryCount == null ? 0 : maxRetryCount);
			configBean.setRetryDelay(retryDelay);
		} else if (configBean instanceof RetryMandatoryConfigBean) {
			if (maxRetryCount != null && maxRetryCount < 1) {
				exception.add(ValidationConstant.Message.RETRY_IS_MANDATORY);
			} else {
				configBean.setMaxRetryCount(maxRetryCount == null ? General.DEFAULT_MAX_RETRY_COUNT : maxRetryCount);
				configBean.setRetryDelay(retryDelay == null || retryDelay < 0 ? General.DEFAULT_RETRY_DELAY : retryDelay);
			}
		} else {
			if (maxRetryCount != null) {
				exception.add(ValidationConstant.Message.CONFIG_NOT_APPLICABLE, General.MAX_RETRY_COUNT);
			}

			if (retryDelay != null) {
				exception.add(ValidationConstant.Message.CONFIG_NOT_APPLICABLE, General.RETRY_DELAY);
			}
		}
	}

	private void validateComponentStatsSettings(LinkedHashMap<String, String> configs, StatsSettingsStore statsSettingsStore) throws StatsSettingsException {
		if (configs.containsKey(General.SOS) && statsSettingsStore == null) {
			exception.add(ValidationConstant.Message.INVALID_TO_SPECIFY_SOS_AND_EOS);
		}
	}

	private StatsStore validateAndGetStatsStore(LinkedHashMap<String, String> configs) throws ImproperValidationException {
		if (ConfigUtil.isAllEmpty(configs, StatsSettings.STATS_TYPE)) {
			return null;
		} else {
			StatsStore statsStore = new StatsStore();

			validateAndSetStatsStore(statsStore, configs);

			return statsStore;
		}
	}

	private void validateAndSetStatsStore(StatsStore statsStore, LinkedHashMap<String, String> configs) throws ImproperValidationException {
		StatsType statsType = StatsType.getInstance(ConfigUtil.getString(configs, StatsSettings.STATS_TYPE));

		if (statsType == null) {
			exception.add(ValidationConstant.Message.KEY_IS_INVALID, StatsSettings.STATS_TYPE);
		}

		statsStore.setStatsType(statsType);
	}

	private void setConfigDetails(ConfigAndMappingConfigBeans configAndMappingConfigBeans, LinkedHashMap<String, String> configs, ArrayList<String> sources,
	        ComponentConfiguration componentConfiguration) {
		configAndMappingConfigBeans.getConfigBean().setName(configs.get(General.NAME));
		configAndMappingConfigBeans.getConfigBean().setSources(sources);
		configAndMappingConfigBeans.getConfigBean().setPersist(ConfigUtil.getBoolean(configs, General.PERSIST, exception, false));
		configAndMappingConfigBeans.getConfigBean()
		        .setSourceStream(ConfigUtil.getString(configs, General.SOURCE_STREAM) == null ? General.DEFAULT_STREAM : configs.get(General.SOURCE_STREAM));
		configAndMappingConfigBeans.getConfigBean().setConfigName(factory.getName());
		configAndMappingConfigBeans.getConfigBean().setSeekDetails(componentConfiguration.getSeekDetails());
	}

	public void setExceptionDetails(SeekDetails seekDetails, LinkedHashMap<String, String> configs) {
		exception.setComponent(factory.getName());
		exception.setComponentName(configs.get(General.NAME));
		exception.setSeekDetails(seekDetails);
	}

	private void validateNameAndSource(ArrayList<String> sources, LinkedHashMap<String, String> configs,
	        HashMap<String, ConfigAndMappingConfigBeans> configAndMappingConfigBeansMap) throws ImproperValidationException {
		String name = configs.get(General.NAME);

		if (configAndMappingConfigBeansMap.containsKey(name)) {
			exception.add(ValidationConstant.Message.DUPLICATE_COMPONENT_NAME, name);
		}

		if (!(factory instanceof SourceFactory)) {
			validateMandatoryField(configs, General.SOURCE);

			int numberOfInputs = sources != null ? sources.size() : 0;

			if (numberOfInputs < factory.getMinInputs() || (factory.getMaxInputs() == null ? false : numberOfInputs > factory.getMaxInputs())) {
				exception.add(ValidationConstant.Message.NUMBER_OF_SOURCES, numberOfInputs + "", factory.getMinInputs().toString(),
				        factory.getMaxInputs().toString());
			}

			for (String source : sources) {
				if (!configAndMappingConfigBeansMap.containsKey(source)) {
					exception.add(ValidationConstant.Message.COMPONENT_NOT_FOUND, source);
				} else {
					ConfigAndMappingConfigBeans configAndMappingConfigBean = configAndMappingConfigBeansMap.get(source);

					if (configAndMappingConfigBean.getConfigBean() instanceof SinkConfigBean) {
						exception.add(ValidationConstant.Message.SOURCE_COMPONENT_SINK, name);
					}
				}
			}
		}
	}

	private LinkedHashMap<Mapping, MappingConfigBean> validateAndGetMappingConfigBeans(LinkedHashMap<String, String> configs, SeekDetails seekDetails)
	        throws ImproperValidationException, InvalidConfigException {
		LinkedHashMap<Mapping, MappingConfigBean> mappingConfigBeans = new LinkedHashMap<Mapping, MappingConfigBean>();

		for (Mapping mapping : Mapping.values()) {
			mappingConfigBeans.put(mapping,
			        mapping.getMappingFactory().getMappingValidator().validateAndSetBean(exception, configs, seekDetails, mapping, factory.getName()));
		}

		return mappingConfigBeans;
	}

	protected void validateMandatoryFields(LinkedHashMap<String, String> configs, String... keys) {
		for (String key : keys) {
			validateMandatoryField(configs, key);
		}
	}

	protected void validateMandatoryField(LinkedHashMap<String, String> configs, String key) {
		if (StringUtils.isBlank(configs.get(key))) {
			exception.add(ValidationConstant.Message.SINGLE_KEY_MANDATORY, key);
		}
	}

	private static boolean isAllEmpty(LinkedHashMap<Mapping, MappingConfigBean> mappingConfigBeans) {
		boolean isAllEmpty = true;

		if (mappingConfigBeans == null) {
			return isAllEmpty;
		}

		for (Mapping mapping : mappingConfigBeans.keySet()) {
			if (mappingConfigBeans.get(mapping) != null) {
				isAllEmpty = false;
				break;
			}
		}

		return isAllEmpty;
	}

	protected void validateMergingConfig(ArrayList<String> configValues1, ArrayList<String> configValues2, String key1, String key2, Integer sizeToMatch,
	        String sizeMatchToKey) {
		if (configValues1 == null && configValues2 == null) {
			exception.add(ValidationConstant.Message.EITHER_OF_THE_KEY_IS_MANDATORY, key1, key2);
		} else {
			boolean sizeMissMatch = false;
			if (sizeToMatch != null && configValues1 != null && configValues1.size() != sizeToMatch) {
				exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, key1, sizeMatchToKey);
				sizeMissMatch = true;
			}

			if (sizeToMatch != null && configValues2 != null && configValues2.size() != sizeToMatch) {
				exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, key2, sizeMatchToKey);
				sizeMissMatch = true;
			}

			if (!sizeMissMatch && configValues1 != null && configValues2 != null) {
				ArrayList<Integer> conflictingIndexes = getConflictingIndexes(configValues1, configValues2);

				if (conflictingIndexes.size() > 0) {
					exception.add(ValidationConstant.Message.CONFLICTING_VALUES_AT, key1, key2, conflictingIndexes.toString());
				}
			}
		}
	}

	protected void validateParellelConfig(ArrayList<String> configValues1, ArrayList<String> configValues2, String key1, String key2) {
		if (configValues1 == null) {
			if (configValues2 != null && !configValues2.isEmpty()) {
				exception.add(ValidationConstant.Message.INVALID_WHEN_OTHER_NOT_PRECENT, key2, key1);
			}

			return;
		} else if (configValues2 == null || configValues2.isEmpty()) {
			return;
		} else {
			if (configValues1.size() != configValues2.size()) {
				exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, key1, key2);
			}

			for (int i = 0; i < configValues1.size(); i++) {
				if (StringUtils.isBlank(configValues1.get(i)) && StringUtils.isNotBlank(configValues2.get(i))) {
					exception.add(ValidationConstant.Message.INVALID_WHEN_OTHER_NOT_PRECENT, key2, key1);
				}
			}
		}
	}

	private ArrayList<Integer> getConflictingIndexes(ArrayList<String> configValues1, ArrayList<String> configValues2) {
		ArrayList<Integer> conflictingIndexes = new ArrayList<Integer>();

		for (int i = 0; i < configValues1.size(); i++) {
			if (!StringUtils.isEmpty(configValues1.get(i)) && !StringUtils.isEmpty(configValues2.get(i))) {
				conflictingIndexes.add(i);
			}
		}

		return conflictingIndexes;
	}

	public abstract ConfigBean validateFactoryConfig(LinkedHashMap<String, String> configs, ConfigAndMappingConfigBeans configAndMappingConfigBeans)
	        throws InvalidConfigException, ImproperValidationException, UnsupportedException;
}
