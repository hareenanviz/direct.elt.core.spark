package elt.core.spark.mapping.function;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
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
public class ContitionalCleansingBigDecimalCallTest extends AnvizentFunctionMappingPositiveCallTest {

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

		return configBean;
	}

	private ArrayList<String> getEquals() {
		ArrayList<String> equals = new ArrayList<>();
		equals.add("");
		equals.add("");
		equals.add("");
		equals.add("552.00");
		equals.add("");

		return equals;
	}

	private ArrayList<String> getNotEquals() {
		ArrayList<String> notEquals = new ArrayList<>();
		notEquals.add("");
		notEquals.add("");
		notEquals.add("");
		notEquals.add("");
		notEquals.add("0");

		return notEquals;
	}

	private ArrayList<String> getFields() {
		ArrayList<String> fields = new ArrayList<>();
		fields.add("Ordered_Qty");
		fields.add("Unit_Price");
		fields.add("Unit_Price_Discount");
		fields.add("Order_Line_Amount");
		fields.add("Order_Line_Discount_Amount");

		return fields;
	}

	private ArrayList<String> getMinValues() {
		ArrayList<String> minValues = new ArrayList<>();
		minValues.add("10");
		minValues.add("");
		minValues.add("");
		minValues.add("");
		minValues.add("");

		return minValues;
	}

	private ArrayList<String> getMaxValues() {
		ArrayList<String> maxValues = new ArrayList<>();
		maxValues.add("20");
		maxValues.add("");
		maxValues.add("");
		maxValues.add("");
		maxValues.add("");

		return maxValues;
	}

	private ArrayList<String> getReplacementValues() {
		ArrayList<String> replacementValues = new ArrayList<>();
		replacementValues.add("12.55565");
		replacementValues.add("150.556");
		replacementValues.add("10.0");
		replacementValues.add("135.556");
		replacementValues.add("15.0");

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
		sourceValues.put("Ordered_Qty", new BigDecimal(10.1525));
		sourceValues.put("Unit_Price", null);
		sourceValues.put("Unit_Price_Discount", new BigDecimal(12.55565));
		sourceValues.put("Order_Line_Amount", new BigDecimal(552).setScale(2));
		sourceValues.put("Order_Line_Discount_Amount", new BigDecimal(112));

		return sourceValues;
	}

	@Override
	public HashMap<String, Object> getExpectedValues() {
		HashMap<String, Object> expectedValues = new HashMap<>();

		expectedValues.put("order_id", 5);
		expectedValues.put("order_name", "AAA");
		expectedValues.put("Ordered_Qty", new BigDecimal(12.55565).setScale(5, RoundingMode.HALF_UP));
		expectedValues.put("Unit_Price", new BigDecimal(150.556).setScale(3, RoundingMode.HALF_UP));
		expectedValues.put("Unit_Price_Discount", new BigDecimal(10.0).setScale(1, RoundingMode.HALF_UP));
		expectedValues.put("Order_Line_Amount", new BigDecimal(135.556).setScale(3, RoundingMode.HALF_UP));
		expectedValues.put("Order_Line_Discount_Amount", new BigDecimal(15.0).setScale(1, RoundingMode.HALF_UP));

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

	@Override
	public LinkedHashMap<String, AnvizentDataType> getStructure() throws UnsupportedException {
		LinkedHashMap<String, AnvizentDataType> structure = new LinkedHashMap<String, AnvizentDataType>();

		structure.put("order_id", new AnvizentDataType(Integer.class));
		structure.put("order_name", new AnvizentDataType(String.class));
		structure.put("Ordered_Qty", new AnvizentDataType(BigDecimal.class));
		structure.put("Unit_Price", new AnvizentDataType(BigDecimal.class));
		structure.put("Unit_Price_Discount", new AnvizentDataType(BigDecimal.class));
		structure.put("Order_Line_Amount", new AnvizentDataType(BigDecimal.class));
		structure.put("Order_Line_Discount_Amount", new AnvizentDataType(BigDecimal.class));

		return structure;
	}
}
