package com.anvizent.elt.core.spark.listener.responder;

import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;

import com.anvizent.elt.core.listener.common.bean.ELTCoreJobListener;
import com.anvizent.elt.core.spark.operation.bean.KafkaRequestSpec;
import com.anvizent.elt.core.spark.operation.config.bean.MLMLookupConfigBean;
import com.anvizent.elt.core.spark.operation.service.MLMLookupService;
import com.anvizent.rest.util.RestUtil;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class MLMTaskKillResponder implements ELTCoreJobListener {

	private MLMLookupConfigBean mlmLookupConfigBean;
	private String druidDataSourceInfoId;

	public MLMTaskKillResponder(MLMLookupConfigBean mlmLookupConfigBean, KafkaRequestSpec kafkaRequestSpec, String druidDataSourceInfoId2) {
		this.mlmLookupConfigBean = mlmLookupConfigBean;
		this.druidDataSourceInfoId = druidDataSourceInfoId2;
	}

	@Override
	public void beforeStart() {
	}

	@Override
	public void afterStop() throws Exception {
		if (mlmLookupConfigBean != null && druidDataSourceInfoId != null) {
			RestUtil restUtil = new RestUtil(true);

			ResponseEntity<String> exchange = restUtil.exchange(mlmLookupConfigBean.getAddToTaskKillListURL(), "", HttpMethod.DELETE,
			        MLMLookupService.getHeadersMap(mlmLookupConfigBean), druidDataSourceInfoId, mlmLookupConfigBean.getUserId(),
			        mlmLookupConfigBean.getClientId());

			MLMLookupService.validateResponse(exchange, mlmLookupConfigBean.getAddToTaskKillListURL(), "addToTaskKillList");
		}
	}

}
