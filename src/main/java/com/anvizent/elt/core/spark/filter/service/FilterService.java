package com.anvizent.elt.core.spark.filter.service;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.config.util.ConfigUtil;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Filter;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.Constants;
import com.anvizent.elt.core.spark.filter.config.bean.FilterByRegExConfigBean;
import com.anvizent.elt.core.spark.filter.config.bean.IFilterConfigBean;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class FilterService {

	public static void setEmitStreams(IFilterConfigBean configBean, LinkedHashMap<String, String> configs, InvalidConfigException exception)
	        throws ImproperValidationException {
		ArrayList<String> emitStreamNames = ConfigUtil.getArrayList(configs, Filter.General.EMIT_STREAM_NAMES, exception);
		if (emitStreamNames == null || emitStreamNames.isEmpty()) {
			emitStreamNames = new ArrayList<>();
			emitStreamNames.add(General.DEFAULT_STREAM);
			configBean.setEmitStreamNames(emitStreamNames);
		} else {
			configBean.setEmitStreamNames(emitStreamNames);
		}
	}

	public static void buildRegularExpressions(FilterByRegExConfigBean regExConfigBean) {
		regExConfigBean.setExpressions(new ArrayList<>());
		regExConfigBean.setArgumentTypes(new ArrayList<>());

		for (int i = 0; i < regExConfigBean.getRegExps().size(); i++) {
			regExConfigBean.getExpressions().add(getIgnoreRowIfNullExpression(regExConfigBean, i) + "java.util.regex.Pattern.matches(\""
			        + regExConfigBean.getRegExps().get(i) + "\", $" + i + ")");
			regExConfigBean.getArgumentTypes().add(Constants.General.DEFAULT_TYPE_FOR_FILTER_BY_REGEX);
		}
	}

	private static String getIgnoreRowIfNullExpression(FilterByRegExConfigBean regExConfigBean, int i) {
		return regExConfigBean.isIgnoreRowIfNull() ? "$" + i + " == null || " : "";
	}
}
