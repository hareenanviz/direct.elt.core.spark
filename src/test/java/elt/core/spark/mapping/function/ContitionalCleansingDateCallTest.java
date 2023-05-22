package elt.core.spark.mapping.function;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;

import com.anvizent.elt.core.lib.AnvizentDataType;
import com.anvizent.elt.core.lib.config.bean.JobDetails;
import com.anvizent.elt.core.lib.config.bean.MappingConfigBean;
import com.anvizent.elt.core.lib.exception.InvalidArgumentsException;
import com.anvizent.elt.core.lib.exception.InvalidRelationException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.lib.function.AnvizentFunction;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.spark.constant.CleansingValidationType;
import com.anvizent.elt.core.spark.mapping.config.bean.ConditionalReplacementCleansingConfigBean;
import com.anvizent.elt.core.spark.mapping.function.ConditionalReplacementCleansingMappingFunction;

import elt.core.spark.function.AnvizentFunctionMappingPositiveCallTest;

/**
 * @author Hareen Bejjanki
 *
 */
public class ContitionalCleansingDateCallTest extends AnvizentFunctionMappingPositiveCallTest {

	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private Date sampleDate = getSampleDate();

	@Override
	public MappingConfigBean getConfigBean() {
		ConditionalReplacementCleansingConfigBean configBean = new ConditionalReplacementCleansingConfigBean();
		configBean.setFields(getFields());
		configBean.setValidationTypes(getValidationTypes());
		configBean.setReplacementValues(getReplacementValues());

		configBean.setMin(getMinValues());
		configBean.setMax(getMaxValues());

		configBean.setEquals(getEquals());
		configBean.setNotEquals(getNotEquals());

		configBean.setDateFormats(getDateFormats());

		return configBean;
	}

	private Date getSampleDate() {
		Date date = null;
		try {
			date = sdf.parse("2017-11-25 00:15:00");
		} catch (ParseException e) {
			e.printStackTrace();
		}

		return date;
	}

	private ArrayList<String> getDateFormats() {
		ArrayList<String> dateFormats = new ArrayList<>();
		dateFormats.add("yyyy-MM-dd HH:mm:ss");
		dateFormats.add("yyyy-MM-dd HH:mm:ss");
		dateFormats.add("yyyy-MM-dd HH:mm:ss");
		dateFormats.add("yyyy-MM-dd HH:mm:ss");
		dateFormats.add("yyyy-MM-dd HH:mm:ss");

		return dateFormats;
	}

	private ArrayList<String> getEquals() {
		ArrayList<String> equals = new ArrayList<>();
		equals.add("");
		equals.add("");
		equals.add("");
		equals.add("2017-11-25 00:15:00");
		equals.add("");

		return equals;
	}

	private ArrayList<String> getNotEquals() {
		ArrayList<String> notEquals = new ArrayList<>();
		notEquals.add("");
		notEquals.add("");
		notEquals.add("");
		notEquals.add("");
		notEquals.add("2017-11-26 00:15:00");

		return notEquals;
	}

	private ArrayList<String> getFields() {
		ArrayList<String> fields = new ArrayList<>();
		fields.add("order_date");
		fields.add("ship_date");
		fields.add("delivered_date");
		fields.add("due_date");
		fields.add("cancelled_date");

		return fields;
	}

	private ArrayList<String> getMinValues() {
		ArrayList<String> minValues = new ArrayList<>();
		minValues.add("2017-11-07 00:00:00");
		minValues.add("");
		minValues.add("");
		minValues.add("");
		minValues.add("");

		return minValues;
	}

	private ArrayList<String> getMaxValues() {
		ArrayList<String> maxValues = new ArrayList<>();
		maxValues.add("2017-12-07 00:00:00");
		maxValues.add("");
		maxValues.add("");
		maxValues.add("");
		maxValues.add("");

		return maxValues;
	}

	private ArrayList<String> getReplacementValues() {
		ArrayList<String> replacementValues = new ArrayList<>();
		replacementValues.add("2018-01-01 00:00:01");
		replacementValues.add("2018-01-02 00:00:02");
		replacementValues.add("2018-01-03 00:00:03");
		replacementValues.add("2018-01-04 00:00:04");
		replacementValues.add("2018-01-05 00:00:05");

		return replacementValues;
	}

	private ArrayList<CleansingValidationType> getValidationTypes() {
		ArrayList<CleansingValidationType> validationTypes = new ArrayList<>();
		validationTypes.add(CleansingValidationType.getInstance("RANGE"));
		validationTypes.add(CleansingValidationType.getInstance("EMPTY"));
		validationTypes.add(CleansingValidationType.getInstance("NOT_EMPTY"));
		validationTypes.add(CleansingValidationType.getInstance("EQUAL"));
		validationTypes.add(CleansingValidationType.getInstance("NOT_EQUAL"));

		return validationTypes;
	}

	@Override
	public LinkedHashMap<String, Object> getSourceValues() {
		LinkedHashMap<String, Object> sourceValues = new LinkedHashMap<>();
		sourceValues.put("order_id", 5);
		sourceValues.put("order_name", "AAA");
		sourceValues.put("order_date", sampleDate);
		sourceValues.put("ship_date", null);
		sourceValues.put("delivered_date", sampleDate);
		sourceValues.put("due_date", sampleDate);
		sourceValues.put("cancelled_date", sampleDate);

		return sourceValues;
	}

	@Override
	public HashMap<String, Object> getExpectedValues() {
		HashMap<String, Object> expectedValues = new HashMap<>();

		expectedValues.put("order_id", 5);
		expectedValues.put("order_name", "AAA");
		expectedValues.put("order_date", getDate("2018-01-01 00:00:01"));
		expectedValues.put("ship_date", getDate("2018-01-02 00:00:02"));
		expectedValues.put("delivered_date", getDate("2018-01-03 00:00:03"));
		expectedValues.put("due_date", getDate("2018-01-04 00:00:04"));
		expectedValues.put("cancelled_date", getDate("2018-01-05 00:00:05"));

		return expectedValues;
	}

	private Date getDate(String sdate) {
		Date date = null;
		try {
			date = sdf.parse(sdate);
		} catch (ParseException e) {
			e.printStackTrace();
		}

		return date;
	}

	@Override
	public void adjustNewStructure(LinkedHashMap<String, AnvizentDataType> newStructure) throws UnsupportedException {
	}

	@Override
	public AnvizentFunction getFunction() throws UnsupportedException, InvalidArgumentsException, InvalidRelationException {
		return new ConditionalReplacementCleansingMappingFunction(null, configBean, structure, newStructure, null, null,
		        getJobDetails(configBean.getComponentName(), configBean.getConfigName(), configBean.getMappingConfigName()));
	}

	public static JobDetails getJobDetails(String componentName, String configName, String internalRDDName) {
		JobDetails jobDetails = new JobDetails(ApplicationBean.getInstance().getSparkAppName(), componentName, configName, internalRDDName);

		return jobDetails;
	}

	@Override
	public LinkedHashMap<String, AnvizentDataType> getStructure() throws UnsupportedException {
		LinkedHashMap<String, AnvizentDataType> structure = new LinkedHashMap<String, AnvizentDataType>();

		structure.put("order_id", new AnvizentDataType(Integer.class));
		structure.put("order_name", new AnvizentDataType(String.class));
		structure.put("order_date", new AnvizentDataType(Date.class));
		structure.put("ship_date", new AnvizentDataType(Date.class));
		structure.put("delivered_date", new AnvizentDataType(Date.class));
		structure.put("due_date", new AnvizentDataType(Date.class));
		structure.put("cancelled_date", new AnvizentDataType(Date.class));

		return structure;
	}
}
