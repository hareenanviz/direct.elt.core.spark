package com.anvizent.elt.core.spark.mapping.validator;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Mapping.DateAndTimeGranularity;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant;
import com.anvizent.elt.core.spark.constant.ConfigConstants.ValidationConstant.Message;
import com.anvizent.elt.core.spark.constant.Granularity;
import com.anvizent.elt.core.spark.mapping.config.bean.DateAndTimeGranularityConfigBean;

/**
 * @author Hareen Bejjanki
 *
 */
public class DateAndTimeGranularityValidator extends MappingValidator {

	private static final long serialVersionUID = 1L;

	@Override
	public MappingConfigBean validateAndSetBean(LinkedHashMap<String, String> configs) throws ImproperValidationException, InvalidConfigException {
		ArrayList<String> fields = ConfigUtil.getArrayList(configs, DateAndTimeGranularity.FIELDS, exception);
		ArrayList<String> granularitiesAsString = ConfigUtil.getArrayList(configs, DateAndTimeGranularity.GRANULARITIES, exception);
		ArrayList<Granularity> granularities = new ArrayList<>();
		boolean allDateFields = ConfigUtil.getBoolean(configs, DateAndTimeGranularity.ALL_DATE_FIELDS, exception);

		if (!allDateFields) {
			if (fields == null || fields.isEmpty()) {
				addException(Message.SINGLE_KEY_MANDATORY, DateAndTimeGranularity.FIELDS);
			} else {
				if (granularitiesAsString == null || granularitiesAsString.isEmpty()) {
					addException(Message.SINGLE_KEY_MANDATORY, DateAndTimeGranularity.GRANULARITIES);
				} else if (fields.size() != granularitiesAsString.size()) {
					addException(Message.SIZE_SHOULD_MATCH, DateAndTimeGranularity.FIELDS, DateAndTimeGranularity.GRANULARITIES);
				} else {
					for (String granularityAsString : granularitiesAsString) {
						Granularity granularity = Granularity.getInstance(granularityAsString);
						if (granularity == null) {
							addException(ValidationConstant.Message.INVALID_VALUE_FOR, granularityAsString, DateAndTimeGranularity.GRANULARITIES);
							break;
						} else {
							granularities.add(granularity);
						}
					}
				}
			}
		} else {
			if (fields != null && !fields.isEmpty()) {
				addException(Message.INVALID_FOR_OTHER, DateAndTimeGranularity.FIELDS, fields.toString(), DateAndTimeGranularity.ALL_DATE_FIELDS,
				        "" + allDateFields);
			}
			if (granularitiesAsString == null || granularitiesAsString.isEmpty()) {
				addException(Message.SINGLE_KEY_MANDATORY, DateAndTimeGranularity.GRANULARITIES);
			} else if (granularitiesAsString != null && granularitiesAsString.size() != 1) {
				addException(Message.ONLY_ONE, DateAndTimeGranularity.GRANULARITIES, DateAndTimeGranularity.ALL_DATE_FIELDS);
			} else {
				Granularity granularity = Granularity.getInstance(granularitiesAsString.get(0));
				if (granularity == null) {
					addException(ValidationConstant.Message.INVALID_VALUE_FOR, granularitiesAsString.get(0), DateAndTimeGranularity.GRANULARITIES);
				} else {
					granularities.add(granularity);
				}
			}
		}

		return new DateAndTimeGranularityConfigBean(fields, granularities, allDateFields);
	}

	@Override
	public String[] getConfigsList() throws ImproperValidationException {
		return new String[] { DateAndTimeGranularity.FIELDS, DateAndTimeGranularity.GRANULARITIES, DateAndTimeGranularity.ALL_DATE_FIELDS };
	}
}
