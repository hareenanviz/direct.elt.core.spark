package com.anvizent.elt.core.spark.mapping.function;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.commons.lang.StringUtils;

import com.anvizent.elt.core.lib.AnvizentAccumulator;
import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;
import com.anvizent.elt.core.lib.exception.DataCorruptedException;
import com.anvizent.elt.core.lib.exception.DateParseException;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
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
import com.anvizent.elt.core.spark.constant.ConfigConstants.Mapping.Constant;
import com.anvizent.elt.core.spark.mapping.config.bean.ConstantConfigBean;
import com.anvizent.elt.core.spark.util.RowUtil;

/**
 * @author Hareen Bejjanki
 * @author apurva.deshmukh - Adds constant specified by user at given position.
 */
public class ConstantMappingFunction extends AnvizentFunction implements MappingFunction {

	private static final long serialVersionUID = 1L;

	public ConstantMappingFunction(ConfigBean configBean, MappingConfigBean mappingConfigBean, LinkedHashMap<String, AnvizentDataType> structure,
	        LinkedHashMap<String, AnvizentDataType> newStructure, ArrayList<AnvizentAccumulator> accumulators, AnvizentVoidFunction errorHandlerSinkFunction,
	        JobDetails jobDetails) throws InvalidArgumentsException {
		super(configBean, mappingConfigBean, structure, newStructure, null, accumulators, errorHandlerSinkFunction, jobDetails);
	}

	@Override
	public HashMap<String, Object> process(HashMap<String, Object> row) throws ValidationViolationException, RecordProcessingException, DataCorruptedException {
		try {
			LinkedHashMap<String, Object> newRow = new LinkedHashMap<String, Object>();
			ConstantConfigBean constantConfigBean = (ConstantConfigBean) mappingConfigBean;

			for (int i = 0; i < constantConfigBean.getFields().size(); i++) {
				putConstant(i, row, newRow);
			}

			return RowUtil.addElements(row, newRow, newStructure);
		} catch (InvalidSituationException | UnsupportedCoerceException exception) {
			throw new DataCorruptedException(exception.getMessage(), exception);
		} catch (InvalidConfigValueException | DateParseException exception) {
			throw new ValidationViolationException(exception.getMessage(), exception);
		}
	}

	private void putConstant(int index, HashMap<String, Object> row, HashMap<String, Object> newRow)
	        throws DateParseException, UnsupportedCoerceException, InvalidSituationException, ImproperValidationException, InvalidConfigValueException {
		ConstantConfigBean constantConfigBean = (ConstantConfigBean) mappingConfigBean;

		String format = getFormat(index);
		if (StringUtils.isBlank(format)) {
			newRow.put(constantConfigBean.getFields().get(index), TypeConversionUtil.dataTypeConversion(constantConfigBean.getValues().get(index), String.class,
			        constantConfigBean.getTypes().get(index), format, null, Constant.CONSTANT_FIELDS_FORMATS, OffsetDateTime.now().getOffset()));
		} else {
			newRow.put(constantConfigBean.getFields().get(index), TypeConversionUtil.dateTypeConversion(constantConfigBean.getValues().get(index), String.class,
			        constantConfigBean.getTypes().get(index), format, Constant.CONSTANT_FIELDS_FORMATS));
		}
	}

	private String getFormat(int index) {
		ConstantConfigBean constantConfigBean = (ConstantConfigBean) mappingConfigBean;

		if (constantConfigBean.getFormats() != null) {
			return constantConfigBean.getFormats().get(index);
		} else {
			return null;
		}
	}

}
