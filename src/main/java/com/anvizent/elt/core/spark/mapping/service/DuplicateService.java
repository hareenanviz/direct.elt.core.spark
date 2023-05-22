package com.anvizent.elt.core.spark.mapping.service;

import org.apache.commons.lang3.StringUtils;

import com.anvizent.elt.core.spark.constant.Constants;

/**
 * @author Hareen Bejjanki
 * @author apurva.deshmukh
 *
 */
public class DuplicateService {

	public static String getKey(String prefix, String suffix) {
		if (StringUtils.isNotEmpty(prefix) && StringUtils.isEmpty(suffix)) {
			return prefix + Constants.General.PLACEHOLDER;
		} else if (StringUtils.isEmpty(prefix) && StringUtils.isNotEmpty(suffix)) {
			return Constants.General.PLACEHOLDER + suffix;
		} else {
			return prefix + Constants.General.PLACEHOLDER + suffix;
		}
	}
}
