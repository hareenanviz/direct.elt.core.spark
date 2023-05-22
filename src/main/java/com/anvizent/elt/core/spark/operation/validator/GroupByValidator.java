package com.anvizent.elt.core.spark.operation.validator;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.config.bean.ConfigAndMappingConfigBeans;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.Aggregations;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Operation.GroupBy;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant.Message;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.operation.config.bean.GroupByConfigBean;
import com.anvizent.elt.core.spark.util.CollectionUtil;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public class GroupByValidator extends Validator {

	private static final long serialVersionUID = 1L;

	public GroupByValidator(Factory factory) {
		super(factory);
	}

	@Override
	public ConfigBean validateFactoryConfig(LinkedHashMap<String, String> configs, ConfigAndMappingConfigBeans configAndMappingConfigBeans)
	        throws InvalidConfigException, ImproperValidationException, UnsupportedException {

		GroupByConfigBean configBean = new GroupByConfigBean();
		validateAndSetBean(configBean, configs);

		return configBean;
	}

	private void validateAndSetBean(GroupByConfigBean configBean, LinkedHashMap<String, String> configs)
	        throws ImproperValidationException, InvalidConfigException {
		validateMandatoryFields(configs, GroupBy.GROUP_BY_FIELDS, GroupBy.AGGREGATIONS, GroupBy.AGGREGATION_FIELDS);

		ArrayList<String> groupByFields = ConfigUtil.getArrayList(configs, GroupBy.GROUP_BY_FIELDS, exception);
		ArrayList<String> aggregations = ConfigUtil.getArrayList(configs, GroupBy.AGGREGATIONS, exception);
		ArrayList<String> joinAggregationDelimeters = ConfigUtil.getArrayList(configs, GroupBy.JOIN_AGGREGATION_DELIMETERS, exception);
		ArrayList<String> aggregationFields = ConfigUtil.getArrayList(configs, GroupBy.AGGREGATION_FIELDS, exception);
		ArrayList<String> aliasNames = ConfigUtil.getArrayList(configs, GroupBy.AGGREGATION_FIELD_ALIAS_NAMES, exception);
		ArrayList<Integer> groupByPositions = ConfigUtil.getArrayListOfIntegers(configs, GroupBy.GROUP_BY_FIELDS_POSITIONS, exception);
		ArrayList<Integer> aggregationPositions = ConfigUtil.getArrayListOfIntegers(configs, GroupBy.AGGREGATION_FIELDS_POSITIONS, exception);

		ArrayList<Integer> precisions = ConfigUtil.getArrayListOfIntegers(configs, General.DECIMAL_PRECISIONS, exception);
		ArrayList<Integer> scales = ConfigUtil.getArrayListOfIntegers(configs, General.DECIMAL_SCALES, exception);

		// TODO something funny, partially fixed
		boolean aggregationFieldsIsNotNull = CollectionUtil.isAllNotEmpty(aggregationFields);
		boolean aggregationsIsNotNull = CollectionUtil.isAllNotEmpty(aggregations);
		boolean groupByFieldsIsNotNull = CollectionUtil.isAllNotNull(groupByFields);
		boolean aggregationFieldPositionsIsNotNull = CollectionUtil.isAllNotNull(aggregationPositions);
		boolean groupByFieldPositionsIsNotNull = CollectionUtil.isAllNotNull(groupByPositions);

		if (!aggregationFieldsIsNotNull) {
			exception.add(ValidationConstant.Message.SINGLE_KEY_MANDATORY, GroupBy.AGGREGATION_FIELDS);
		}

		if (!aggregationsIsNotNull) {
			exception.add(ValidationConstant.Message.SINGLE_KEY_MANDATORY, GroupBy.AGGREGATIONS);
		}

		if (!groupByFieldsIsNotNull) {
			exception.add(ValidationConstant.Message.SINGLE_KEY_MANDATORY, GroupBy.GROUP_BY_FIELDS);
		}

		if (groupByFieldPositionsIsNotNull && groupByPositions.size() != groupByFields.size()) {
			exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, GroupBy.GROUP_BY_FIELDS, GroupBy.GROUP_BY_FIELDS_POSITIONS);
		}

		if (joinAggregationDelimeters != null && joinAggregationDelimeters.size() != aggregations.size()) {
			exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, GroupBy.GROUP_BY_FIELDS, GroupBy.GROUP_BY_FIELDS_POSITIONS);
		}

		for (int i = 0; i < aggregations.size(); i++) {
			Aggregations aggregation = Aggregations.getInstance(aggregations.get(i));

			if (aggregation == null) {
				exception.add(ValidationConstant.Message.INVALID_AGGREGATION, aggregations.get(i), GroupBy.AGGREGATIONS);
			} else if (aggregation.equals(Aggregations.JOIN_BY_DELIM)
			        && (joinAggregationDelimeters == null || joinAggregationDelimeters.get(i) == null || joinAggregationDelimeters.get(i).isEmpty())) {
				exception.add(ValidationConstant.Message.IS_MANDATORY_FOR_VALUE, GroupBy.JOIN_AGGREGATION_DELIMETERS, GroupBy.AGGREGATIONS,
				        Aggregations.JOIN_BY_DELIM.name());
			}
		}

		if (aggregationsIsNotNull && aggregationFields.size() != aggregations.size()) {
			exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, GroupBy.AGGREGATION_FIELDS, GroupBy.AGGREGATIONS);
		}

		if (aliasNames == null || aliasNames.isEmpty()) {
			exception.add(ValidationConstant.Message.SINGLE_KEY_MANDATORY, GroupBy.AGGREGATION_FIELD_ALIAS_NAMES);
		} else if (aggregationFieldsIsNotNull && aggregationFields.size() != aliasNames.size()) {
			exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, GroupBy.AGGREGATION_FIELDS, GroupBy.AGGREGATION_FIELD_ALIAS_NAMES);
		}

		if (aggregationFieldsIsNotNull && aggregationFieldPositionsIsNotNull && aggregationPositions.size() != aggregationFields.size()) {
			exception.add(ValidationConstant.Message.SIZE_SHOULD_MATCH, GroupBy.AGGREGATION_FIELDS, GroupBy.AGGREGATION_FIELDS_POSITIONS);
		}

		if (aggregationFieldsIsNotNull && ConfigUtil.isAllNotEmpty(configs, General.DECIMAL_PRECISIONS) && precisions.size() != aggregationFields.size()) {
			exception.add(Message.SIZE_SHOULD_MATCH, General.DECIMAL_PRECISIONS, GroupBy.AGGREGATION_FIELDS);
		}

		if (aggregationFieldsIsNotNull && ConfigUtil.isAllNotEmpty(configs, General.DECIMAL_SCALES) && scales.size() != aggregationFields.size()) {
			exception.add(Message.SIZE_SHOULD_MATCH, General.DECIMAL_SCALES, GroupBy.AGGREGATION_FIELDS);
		}

		if ((aggregationFieldPositionsIsNotNull && !groupByFieldPositionsIsNotNull)
		        || (!aggregationFieldPositionsIsNotNull && groupByFieldPositionsIsNotNull)) {
			exception.add(ValidationConstant.Message.EITHER_BOTH_ARE_MANDATORY_OR_NONE, GroupBy.GROUP_BY_FIELDS_POSITIONS,
			        GroupBy.AGGREGATION_FIELDS_POSITIONS);
		}

		if (groupByFields != null && aliasNames != null) {
			for (String aliasName : aliasNames) {
				if (groupByFields.contains(aliasName)) {
					exception.add(ValidationConstant.Message.VALUE_ALREADY_PRESENT_IN_ANOTHER_CONFIG, aliasName, GroupBy.GROUP_BY_FIELDS);
				}
			}
		}

		configBean.setGroupByFields(groupByFields);
		configBean.setAggregationFields(aggregationFields);
		configBean.setAggregations(aggregations == null ? null : Aggregations.getInstances(aggregations));
		configBean.setJoinAggregationsDelimeter(joinAggregationDelimeters);
		configBean.setAliasNames(aliasNames);
		configBean.setGroupByFieldPositions(groupByPositions);
		configBean.setAggregationFieldPositions(aggregationPositions);
		configBean.setPrecisions(precisions);
		configBean.setScales(scales);
	}

}
