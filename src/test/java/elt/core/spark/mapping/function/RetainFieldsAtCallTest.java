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
import com.anvizent.elt.core.spark.mapping.config.bean.RetainConfigBean;
import com.anvizent.elt.core.spark.mapping.function.RetainMappingFunction;

import elt.core.spark.function.AnvizentFunctionMappingPositiveCallTest;

/**
 * @author Hareen Bejjanki
 *
 */
public class RetainFieldsAtCallTest extends AnvizentFunctionMappingPositiveCallTest {

	private Date now = new Date();

	@Override
	public MappingConfigBean getConfigBean() {
		return new RetainConfigBean(getRetainFields(), null, getRetainFieldsAt(), null);
	}

	private ArrayList<String> getRetainFields() {
		ArrayList<String> retainFields = new ArrayList<>();
		retainFields.add("ID");
		retainFields.add("Company");
		retainFields.add("OrderDate");
		retainFields.add("Country");
		retainFields.add("CustomerName");
		retainFields.add("PlantName");

		return retainFields;
	}

	private ArrayList<Integer> getRetainFieldsAt() {
		ArrayList<Integer> retainFieldsAt = new ArrayList<>();
		retainFieldsAt.add(-6);
		retainFieldsAt.add(-5);
		retainFieldsAt.add(-4);
		retainFieldsAt.add(-3);
		retainFieldsAt.add(-2);
		retainFieldsAt.add(-1);

		return retainFieldsAt;
	}

	@Override
	public LinkedHashMap<String, Object> getSourceValues() {
		LinkedHashMap<String, Object> sourceValues = new LinkedHashMap<>();
		sourceValues.put("ID", 101);
		sourceValues.put("Company", "USA");
		sourceValues.put("WO", 6984451);
		sourceValues.put("OrderDate", now);
		sourceValues.put("PONumber", 24143);
		sourceValues.put("Country", "UNITED STATES");
		sourceValues.put("CustomerName", "VTI OF GEORGIA  INC.");
		sourceValues.put("PlantName", "Statesville NC (1130)");
		sourceValues.put("SalesRepName", "Mario Edwards");

		return sourceValues;
	}

	@Override
	public HashMap<String, Object> getExpectedValues() {
		HashMap<String, Object> expectedValues = new HashMap<>();

		expectedValues.put("PlantName", "Statesville NC (1130)");
		expectedValues.put("CustomerName", "VTI OF GEORGIA  INC.");
		expectedValues.put("Country", "UNITED STATES");
		expectedValues.put("OrderDate", now);
		expectedValues.put("Company", "USA");
		expectedValues.put("ID", 101);

		return expectedValues;
	}

	@Override
	public LinkedHashMap<String, AnvizentDataType> getNewStructure() throws UnsupportedException {
		newStructure = new LinkedHashMap<>();
		newStructure.put("PlantName", structure.get("PlantName"));
		newStructure.put("CustomerName", structure.get("CustomerName"));
		newStructure.put("Country", structure.get("Country"));
		newStructure.put("OrderDate", structure.get("OrderDate"));
		newStructure.put("Company", structure.get("Company"));
		newStructure.put("ID", structure.get("ID"));

		return newStructure;
	}

	@Override
	public void adjustNewStructure(LinkedHashMap<String, AnvizentDataType> newStructure) throws UnsupportedException {
	}

	@Override
	public AnvizentFunction getFunction() throws UnsupportedException, InvalidArgumentsException, InvalidRelationException {
		return new RetainMappingFunction(null, configBean, structure, newStructure, null, null,
		        getJobDetails(configBean.getComponentName(), configBean.getConfigName(), configBean.getMappingConfigName()));
	}

	public static JobDetails getJobDetails(String componentName, String configName, String internalRDDName) {
		JobDetails jobDetails = new JobDetails(ApplicationBean.getInstance().getSparkAppName(), componentName, configName, internalRDDName);

		return jobDetails;
	}
}
