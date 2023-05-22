package com.anvizent.elt.core.spark.operation.validator;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.Join;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant;
import com.anvizent.elt.core.spark.constant.JoinMode;
import com.anvizent.elt.core.spark.constant.JoinType;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.operation.config.bean.JoinConfigBean;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public class JoinValidator extends Validator {

	private static final long serialVersionUID = 1L;

	public JoinValidator(Factory factory) {
		super(factory);
	}

	@Override
	public ConfigBean validateFactoryConfig(LinkedHashMap<String, String> configs, ConfigAndMappingConfigBeans configAndMappingConfigBeans)
	        throws InvalidConfigException, ImproperValidationException, UnsupportedException {
		JoinConfigBean configBean = new JoinConfigBean();

		validateMandatoryFields(configs, Join.LEFT_HAND_SIDE_FIELDS, Join.RIGHT_HAND_SIDE_FIELDS);

		validateAndSetBean(configBean, configs);

		return configBean;
	}

	private void validateAndSetBean(JoinConfigBean configBean, LinkedHashMap<String, String> configs) throws ImproperValidationException {
		String joinType = ConfigUtil.getString(configs, Join.JOIN_TYPE);
		JoinMode joinMode = JoinMode.getInstance(ConfigUtil.getString(configs, Join.JOIN_MODE));
		ArrayList<String> lhsFields = ConfigUtil.getArrayList(configs, Join.LEFT_HAND_SIDE_FIELDS, exception);
		ArrayList<String> rhsFields = ConfigUtil.getArrayList(configs, Join.RIGHT_HAND_SIDE_FIELDS, exception);
		String lhsPrefix = ConfigUtil.getString(configs, Join.LEFT_HAND_SIDE_FIELD_PREFIX);
		String rhsPrefix = ConfigUtil.getString(configs, Join.RIGHT_HAND_SIDE_FIELD_PREFIX);
		Long maxRowsForBroadcast = ConfigUtil.getLong(configs, Join.MAX_ROWS_FOR_BROADCAST, exception);
		Long maxSizeForBroadcast = ConfigUtil.getLong(configs, Join.MAX_SIZE_FOR_BROADCAST, exception);

		if (joinType == null) {
			exception.add(ValidationConstant.Message.SINGLE_KEY_MANDATORY, Join.JOIN_TYPE);
		}

		if (lhsFields != null && rhsFields != null) {
			if (lhsFields.size() != rhsFields.size()) {
				exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, Join.LEFT_HAND_SIDE_FIELDS, Join.RIGHT_HAND_SIDE_FIELDS);
			}
		}

		if (lhsPrefix != null && rhsPrefix != null) {
			exception.add(ValidationConstant.Message.EITHER_ONE_ARE_MANDATORY_OR_NONE, Join.LEFT_HAND_SIDE_FIELD_PREFIX, Join.RIGHT_HAND_SIDE_FIELD_PREFIX);
		}

		if (JoinType.getInstance(joinType) == null) {
			exception.add(ValidationConstant.Message.KEY_IS_INVALID, Join.JOIN_TYPE);
		}

		if (!JoinMode.REGULAR.equals(joinMode) && !JoinMode.BROADCAST_LEFT.equals(joinMode) && !JoinMode.BROADCAST_RIGHT.equals(joinMode)
		        && (maxRowsForBroadcast == null || maxRowsForBroadcast <= 1) && (maxSizeForBroadcast == null || maxSizeForBroadcast <= 1)) {
			exception.add(ValidationConstant.Message.EITHER_OF_IS_MANDATORY_FOR_VALUE, Join.MAX_ROWS_FOR_BROADCAST, Join.MAX_SIZE_FOR_BROADCAST, Join.JOIN_MODE,
			        joinMode);
		}

		if (JoinMode.BROADCAST_RIGHT.equals(joinMode) && !JoinType.LEFT_OUTER_JOIN.name().equalsIgnoreCase(joinType)
		        && !JoinType.SIMPLE_JOIN.name().equalsIgnoreCase(joinType)) {
			exception.add(ValidationConstant.Message.INVALID_FOR_OTHER, Join.JOIN_TYPE, joinType, Join.JOIN_MODE, joinMode);
		}

		configBean.setJoinType(JoinType.getInstance(joinType));
		configBean.setJoinMode(joinMode);
		configBean.setLHSFields(lhsFields);
		configBean.setRHSFields(rhsFields);
		configBean.setLHSPrefix(lhsPrefix == null ? "" : lhsPrefix);
		configBean.setRHSPrefix(rhsPrefix == null ? "" : rhsPrefix);
	}

}
