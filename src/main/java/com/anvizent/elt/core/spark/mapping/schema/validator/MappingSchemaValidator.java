/**
 * 
 */
package com.anvizent.elt.core.spark.mapping.schema.validator;

import java.io.Serializable;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;

/**
 * @author Hareen Bejjanki
 *
 */
public interface MappingSchemaValidator extends Serializable {

	void validate(MappingConfigBean mappingConfigBean, LinkedHashMap<String, AnvizentDataType> structure, InvalidConfigException invalidConfigException)
	        throws InvalidConfigException;

}
