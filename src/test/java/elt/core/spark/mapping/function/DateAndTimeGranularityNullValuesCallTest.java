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
import com.anvizent.elt.core.spark.constant.Granularity;
import com.anvizent.elt.core.spark.mapping.config.bean.DateAndTimeGranularityConfigBean;
import com.anvizent.elt.core.spark.mapping.function.DateAndTimeGranularityMappingFunction;

import elt.core.spark.function.AnvizentFunctionMappingPositiveCallTest;

/**
 * @author Hareen Bejjanki
 *
 */
public class DateAndTimeGranularityNullValuesCallTest extends AnvizentFunctionMappingPositiveCallTest {

	private Date now = getDate("2015-05-25 12:53:49.5");

	@Override
	public MappingConfigBean getConfigBean() {

		return new DateAndTimeGranularityConfigBean(getFields(), getGranularities(), false);
	}

	private Date getDate(String sDate) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		Date date = null;

		try {
			date = sdf.parse(sDate);
		} catch (ParseException e) {
			e.printStackTrace();
		}

		return date;
	}

	private ArrayList<Granularity> getGranularities() {
		ArrayList<Granularity> granularities = new ArrayList<>();
		granularities.add(Granularity.SECOND);
		granularities.add(Granularity.MINUTE);
		granularities.add(Granularity.HOUR);
		granularities.add(Granularity.DAY);
		granularities.add(Granularity.MONTH);
		granularities.add(Granularity.YEAR);
		granularities.add(Granularity.SECOND);
		granularities.add(Granularity.MINUTE);
		granularities.add(Granularity.HOUR);

		return granularities;
	}

	private ArrayList<String> getFields() {
		ArrayList<String> fields = new ArrayList<>();
		fields.add("Sales_Order_Date");
		fields.add("Shipped_Date");
		fields.add("Due_Date");
		fields.add("Cancelled_Date");
		fields.add("Last_Invoice_Date");
		fields.add("BDateTime01");
		fields.add("BDateTime02");
		fields.add("BDateTime03");
		fields.add("BDateTime04");

		return fields;
	}

	@Override
	public LinkedHashMap<String, Object> getSourceValues() {
		LinkedHashMap<String, Object> sourceValues = new LinkedHashMap<>();
		sourceValues.put("order_id", 5);
		sourceValues.put("Sales_Order_Date", now);
		sourceValues.put("Shipped_Date", null);
		sourceValues.put("Due_Date", now);
		sourceValues.put("Cancelled_Date", now);
		sourceValues.put("Last_Invoice_Date", now);
		sourceValues.put("BDateTime01", now);
		sourceValues.put("BDateTime02", null);
		sourceValues.put("BDateTime03", now);
		sourceValues.put("BDateTime04", now);

		return sourceValues;
	}

	@Override
	public HashMap<String, Object> getExpectedValues() {
		HashMap<String, Object> expectedValues = new HashMap<>();

		expectedValues.put("order_id", 5);
		expectedValues.put("Sales_Order_Date", getDate("2015-05-25 12:53:49.0"));
		expectedValues.put("Shipped_Date", null);
		expectedValues.put("Due_Date", getDate("2015-05-25 12:00:00.0"));
		expectedValues.put("Cancelled_Date", getDate("2015-05-25 00:00:00.0"));
		expectedValues.put("Last_Invoice_Date", getDate("2015-05-01 00:00:00.0"));
		expectedValues.put("BDateTime01", getDate("2015-01-01 00:00:00.0"));
		expectedValues.put("BDateTime02", null);
		expectedValues.put("BDateTime03", getDate("2015-05-25 12:53:00.0"));
		expectedValues.put("BDateTime04", getDate("2015-05-25 12:00:00.0"));

		return expectedValues;
	}

	@Override
	public void adjustNewStructure(LinkedHashMap<String, AnvizentDataType> newStructure) throws UnsupportedException {
	}

	@Override
	public AnvizentFunction getFunction() throws UnsupportedException, InvalidArgumentsException, InvalidRelationException {
		return new DateAndTimeGranularityMappingFunction(null, configBean, structure, newStructure, null, null,
		        getJobDetails(configBean.getComponentName(), configBean.getConfigName(), configBean.getMappingConfigName()));
	}

	public static JobDetails getJobDetails(String componentName, String configName, String internalRDDName) {
		JobDetails jobDetails = new JobDetails(ApplicationBean.getInstance().getSparkAppName(), componentName, configName, internalRDDName);

		return jobDetails;
	}

	@Override
	public LinkedHashMap<String, AnvizentDataType> getStructure() throws UnsupportedException {
		LinkedHashMap<String, AnvizentDataType> structure = new LinkedHashMap<>();
		structure.put("order_id", new AnvizentDataType(Date.class));
		structure.put("Sales_Order_Date", new AnvizentDataType(Date.class));
		structure.put("Shipped_Date", new AnvizentDataType(Date.class));
		structure.put("Due_Date", new AnvizentDataType(Date.class));
		structure.put("Cancelled_Date", new AnvizentDataType(Date.class));
		structure.put("Last_Invoice_Date", new AnvizentDataType(Date.class));
		structure.put("BDateTime01", new AnvizentDataType(Date.class));
		structure.put("BDateTime02", new AnvizentDataType(Date.class));
		structure.put("BDateTime03", new AnvizentDataType(Date.class));
		structure.put("BDateTime04", new AnvizentDataType(Date.class));

		return structure;
	}
}
