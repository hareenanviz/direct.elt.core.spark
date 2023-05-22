package elt.core.spark.mapping.function;

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
public class ContitionalCleansingCallTest extends AnvizentFunctionMappingPositiveCallTest {

	private Date now = new Date();

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

		configBean.setMatchesRegex(getMatchesRegexp());
		configBean.setNotMatchesRegex(getNotMatchesRegexp());

		configBean.setExpressions(getExpressions());
		configBean.setArgumentFields(getArgumentFields());
		configBean.setArgumentTypes(getArgumentTypes());

		return configBean;
	}

	private ArrayList<String> getMatchesRegexp() {
		ArrayList<String> matchesRegexp = new ArrayList<>();
		matchesRegexp.add("");
		matchesRegexp.add("");
		matchesRegexp.add("");
		matchesRegexp.add("");
		matchesRegexp.add("");
		matchesRegexp.add("[a-zA-Z0-9.]+@[a-zA-Z0-9]+.[a-zA-Z]{2,3}");
		matchesRegexp.add("");
		matchesRegexp.add("");

		return matchesRegexp;
	}

	private ArrayList<String> getNotMatchesRegexp() {
		ArrayList<String> notMatchesRegexp = new ArrayList<>();
		notMatchesRegexp.add("");
		notMatchesRegexp.add("");
		notMatchesRegexp.add("");
		notMatchesRegexp.add("");
		notMatchesRegexp.add("");
		notMatchesRegexp.add("");
		notMatchesRegexp.add("[A-X]{1}");
		notMatchesRegexp.add("");

		return notMatchesRegexp;
	}

	private ArrayList<String> getExpressions() {
		ArrayList<String> expressions = new ArrayList<>();
		expressions.add("");
		expressions.add("");
		expressions.add("");
		expressions.add("");
		expressions.add("");
		expressions.add("");
		expressions.add("");
		expressions.add("$0.compareTo(153.4f) > 0");

		return expressions;
	}

	private ArrayList<String> getArgumentFields() {
		ArrayList<String> argumentFields = new ArrayList<>();
		argumentFields.add("total_amount");

		return argumentFields;
	}

	private ArrayList<Class<?>> getArgumentTypes() {
		ArrayList<Class<?>> argumentTypes = new ArrayList<>();
		argumentTypes.add(java.lang.Float.class);

		return argumentTypes;
	}

	private ArrayList<String> getEquals() {
		ArrayList<String> equals = new ArrayList<>();
		equals.add("");
		equals.add("");
		equals.add("");
		equals.add("12");
		equals.add("");
		equals.add("");
		equals.add("");
		equals.add("");

		return equals;
	}

	private ArrayList<String> getNotEquals() {
		ArrayList<String> notEquals = new ArrayList<>();
		notEquals.add("");
		notEquals.add("");
		notEquals.add("");
		notEquals.add("");
		notEquals.add("55.560");
		notEquals.add("");
		notEquals.add("");
		notEquals.add("");

		return notEquals;
	}

	private ArrayList<String> getFields() {
		ArrayList<String> fields = new ArrayList<>();
		fields.add("order_id");
		fields.add("added_user");
		fields.add("order_name");
		fields.add("order_number");
		fields.add("discount");
		fields.add("customer_mail");
		fields.add("paid");
		fields.add("total_amount");

		return fields;
	}

	private ArrayList<String> getMinValues() {
		ArrayList<String> minValues = new ArrayList<>();
		minValues.add("1");
		minValues.add("");
		minValues.add("");
		minValues.add("");
		minValues.add("");
		minValues.add("");
		minValues.add("");
		minValues.add("");

		return minValues;
	}

	private ArrayList<String> getMaxValues() {
		ArrayList<String> maxValues = new ArrayList<>();
		maxValues.add("10");
		maxValues.add("");
		maxValues.add("");
		maxValues.add("");
		maxValues.add("");
		maxValues.add("");
		maxValues.add("");
		maxValues.add("");

		return maxValues;
	}

	private ArrayList<String> getReplacementValues() {
		ArrayList<String> replacementValues = new ArrayList<>();
		replacementValues.add("0");
		replacementValues.add("unknown");
		replacementValues.add("ZZZ");
		replacementValues.add("0");
		replacementValues.add("0.00");
		replacementValues.add("ZZZ@dummy.com");
		replacementValues.add("N");
		replacementValues.add("100.0");

		return replacementValues;
	}

	private ArrayList<CleansingValidationType> getValidationTypes() {
		ArrayList<CleansingValidationType> validationTypes = new ArrayList<>();
		validationTypes.add(CleansingValidationType.getInstance("RANGE"));
		validationTypes.add(CleansingValidationType.getInstance("EMPTY"));
		validationTypes.add(CleansingValidationType.getInstance("NOT_EMPTY"));
		validationTypes.add(CleansingValidationType.getInstance("EQUAL"));
		validationTypes.add(CleansingValidationType.getInstance("NOT_EQUAL"));
		validationTypes.add(CleansingValidationType.getInstance("MATCHES_REGEX"));
		validationTypes.add(CleansingValidationType.getInstance("NOT_MATCHES_REGEX"));
		validationTypes.add(CleansingValidationType.getInstance("CUSTOM_EXPRESSION"));

		return validationTypes;
	}

	@Override
	public LinkedHashMap<String, Object> getSourceValues() {
		LinkedHashMap<String, Object> sourceValues = new LinkedHashMap<>();
		sourceValues.put("order_id", 5);
		sourceValues.put("added_user", "");
		sourceValues.put("order_name", "AAA");
		sourceValues.put("order_number", 12);
		sourceValues.put("discount", 55.559);
		sourceValues.put("reference_number", 8954565);
		sourceValues.put("order_date", now);
		sourceValues.put("customer_address", "");
		sourceValues.put("customer_mail", "apurva09@something.com");
		sourceValues.put("customer_name", "XXX");
		sourceValues.put("paid", 'Y');
		sourceValues.put("total_amount", 153.5f);

		return sourceValues;
	}

	@Override
	public HashMap<String, Object> getExpectedValues() {
		HashMap<String, Object> expectedValues = new HashMap<>();

		expectedValues.put("order_id", 0);
		expectedValues.put("added_user", "unknown");
		expectedValues.put("order_name", "ZZZ");
		expectedValues.put("order_number", 0);
		expectedValues.put("discount", 0.0);
		expectedValues.put("reference_number", 8954565);
		expectedValues.put("order_date", now);
		expectedValues.put("customer_address", "");
		expectedValues.put("customer_mail", "ZZZ@dummy.com");
		expectedValues.put("customer_name", "XXX");
		expectedValues.put("paid", 'N');
		expectedValues.put("total_amount", 100.0f);

		return expectedValues;
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

}
