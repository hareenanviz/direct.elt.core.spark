package com.anvizent.elt.core.spark.mapping.function;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;
import com.anvizent.elt.core.lib.exception.DataCorruptedException;
import com.anvizent.elt.core.lib.exception.DateParseException;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.InvalidConfigValueException;
import com.anvizent.elt.core.lib.exception.InvalidSituationException;
import com.anvizent.elt.core.lib.exception.RecordProcessingException;
import com.anvizent.elt.core.lib.exception.UnsupportedCoerceException;
import com.anvizent.elt.core.lib.exception.ValidationViolationException;
import com.anvizent.elt.core.lib.function.AnvizentFunction;
import com.anvizent.elt.core.lib.function.AnvizentVoidFunction;
import com.anvizent.elt.core.lib.function.MappingFunction;
import com.anvizent.elt.core.lib.util.TypeConversionUtil;
import com.anvizent.elt.core.spark.constant.Constants;
import com.anvizent.elt.core.spark.constant.Constants.General;
import com.anvizent.elt.core.spark.mapping.config.bean.DuplicateConfigBean;
import com.anvizent.elt.core.spark.mapping.service.DuplicateService;
import com.anvizent.elt.core.spark.util.RowUtil;

/**
 * @author Hareen Bejjanki
 * @author apurva.deshmukh -- Duplicate existing fields and append at - begin/
 *         previous to particular field/ next to particular field/ end.
 *
 */
public class DuplicateMappingFunction extends AnvizentFunction implements MappingFunction {

	private static final long serialVersionUID = 1L;

	public DuplicateMappingFunction(ConfigBean configBean, MappingConfigBean mappingConfigBean, LinkedHashMap<String, AnvizentDataType> structure,
	        LinkedHashMap<String, AnvizentDataType> newStructure, ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction errorHandlerSinkFunction,
	        JobDetails jobDetails) throws InvalidArgumentsException {
		super(configBean, mappingConfigBean, structure, newStructure, null, accumulators, errorHandlerSinkFunction, jobDetails);
	}

	@Override
	public HashMap<String, Object> process(HashMap<String, Object> row) throws ValidationViolationException, RecordProcessingException, DataCorruptedException {
		DuplicateConfigBean duplicateConfigBean = (DuplicateConfigBean) mappingConfigBean;
		String newKey = DuplicateService.getKey(duplicateConfigBean.getPrefix(), duplicateConfigBean.getSuffix());
		HashMap<String, Object> newValues = new HashMap<String, Object>();

		try {
			for (Entry<String, Object> rowEntry : row.entrySet()) {
				Object fieldValue = rowEntry.getValue();
				if (fieldValue != null && !Constants.Type.DIMENSIONS.contains(fieldValue.getClass())) {
					newValues.put(StringUtils.replace(newKey, General.PLACEHOLDER, rowEntry.getKey()), TypeConversionUtil.dataTypeConversion(fieldValue,
					        fieldValue.getClass(), String.class, null, null, null, OffsetDateTime.now().getOffset()));

				}
			}

			return RowUtil.addElements(row, newValues, newStructure);
		} catch (InvalidSituationException | UnsupportedCoerceException exception) {
			throw new DataCorruptedException(exception.getMessage(), exception);
		} catch (InvalidConfigValueException | DateParseException exception) {
			throw new ValidationViolationException(exception.getMessage(), exception);
		}
	}
}