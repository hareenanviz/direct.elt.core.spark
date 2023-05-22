/**
 * 
 */
package com.anvizent.elt.core.spark.schema.validator;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeoutException;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.CoerceException;
import com.anvizent.elt.core.lib.exception.DateParseException;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.InvalidConfigValueException;
import com.anvizent.elt.core.lib.exception.InvalidSituationException;
import com.anvizent.elt.core.lib.exception.UnimplementedException;
import com.anvizent.elt.core.lib.exception.UnsupportedCoerceException;
import com.anvizent.elt.core.listener.common.exception.InvalidConfigException;
import com.anvizent.elt.core.spark.exception.InvalidInputForConfigException;

/**
 * @author Hareen Bejjanki
 *
 */
public abstract class SchemaValidator implements Serializable {
	private static final long serialVersionUID = 1L;

	public abstract void validate(ConfigBean configBean, int sourceIndex, LinkedHashMap<String, AnvizentDataType> structure,
	        InvalidConfigException invalidConfigException)
	        throws ImproperValidationException, UnimplementedException, ClassNotFoundException, SQLException, InvalidInputForConfigException, TimeoutException,
	        DateParseException, UnsupportedCoerceException, InvalidSituationException, InvalidConfigValueException, CoerceException;

}
