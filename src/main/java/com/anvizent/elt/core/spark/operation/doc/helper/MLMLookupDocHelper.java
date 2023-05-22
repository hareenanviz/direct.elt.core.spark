package com.anvizent.elt.core.spark.operation.doc.helper;

import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.MLM;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.factory.Factory;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class MLMLookupDocHelper extends DocHelper {

	public MLMLookupDocHelper(Factory factory) throws InvalidParameter {
		super(factory);
	}

	@Override
	public String[] getDescription() {
		return new String[] { "MLM(v7 or above) LookUp for increamental load into druid." };
	}

	@Override
	public void addConfigDescriptions() throws InvalidParameter {
		configDescriptionUtil.addConfigDescription(MLM.QUERY_URL, General.YES, "", new String[] { "For increamental load to query to druid.",
				"Example: 192.168.0.135:9099/v6/dataSources/query?userId={userId}&clientId={clientId}" });
		configDescriptionUtil.addConfigDescription(MLM.INITIATE_URL, General.YES, "", new String[] { "For initiating kafka supervisor.",
				"Example: 192.168.0.135:9099/v6/dataSources/{dataSource}/addToUploadList?userId={userId}&clientId={clientId}&type={type}&incrementalLoad={incrementalLoad}" });
		configDescriptionUtil.addConfigDescription(MLM.USER_ID, General.YES, "", new String[] { "User id." });
		configDescriptionUtil.addConfigDescription(MLM.CLIENT_ID, General.YES, "", new String[] { "Client id." });
		configDescriptionUtil.addConfigDescription(MLM.DATA_SOURCE, General.YES, "", new String[] { "The lookup Data source name." });
		configDescriptionUtil.addConfigDescription(MLM.IS_INCREMENTAL, General.NO, "false", new String[] { "Is a increamental load into druid." });
		configDescriptionUtil.addConfigDescription(MLM.KAFKA_BOOTSTRAP_SERVERS, General.YES, "",
				new String[] { "Kafka bootstrap servers from which druid is to pull data." });
		configDescriptionUtil.addConfigDescription(MLM.KAFKA_TOPIC, General.YES, "", new String[] { "Kafka topic name from which druid is to pull data." });
		configDescriptionUtil.addConfigDescription(MLM.TASK_COUNT, General.NO, "3", new String[] { "Supervisor ioConfig task count." }, "", "Long");
		configDescriptionUtil.addConfigDescription(MLM.REPLICAS, General.NO, "3", new String[] { "Supervisor ioConfig replicas" }, "", "Long");
		configDescriptionUtil.addConfigDescription(MLM.TASK_DURATION, General.NO, "false", new String[] { "Supervisor ioConfig task duration" });
		configDescriptionUtil.addConfigDescription(MLM.PRIVATE_KEY, General.YES, "", new String[] { "For encrypting user id, client id." });
		configDescriptionUtil.addConfigDescription(MLM.IV, General.YES, "", new String[] { "For encrypting user id, client id." });
		configDescriptionUtil.addConfigDescription(MLM.KEY_FIELDS, General.NO, "",
				new String[] { "Key fields based on which to decide whether to insert or update. If " + MLM.IS_INCREMENTAL + " is " + true
						+ " and key fields are not provided, this component will consider all dimensions as Key fields." });
		configDescriptionUtil.addConfigDescription(MLM.DRUID_TIME_FORMAT, General.NO, MLM.DEFAULT_DRUID_TIME_FORMAT,
				new String[] { "For encrypting user id, client id." });
	}

}
