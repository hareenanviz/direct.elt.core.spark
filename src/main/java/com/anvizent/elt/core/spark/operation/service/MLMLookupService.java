package com.anvizent.elt.core.spark.operation.service;

import java.util.ArrayList;
import java.util.HashMap;

import org.springframework.http.ResponseEntity;

import com.anvizent.elt.core.spark.constant.ConfigConstants.MLM;
import com.anvizent.elt.core.spark.operation.config.bean.MLMLookupConfigBean;
import com.anvizent.elt.core.spark.operation.exception.MLMResponseException;
import com.anvizent.encryptor.AnvizentEncryptor;
import com.anvizent.rest.util.RestUtil;

import flexjson.JSONDeserializer;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class MLMLookupService {

	public static HashMap<String, String> getHeadersMap(MLMLookupConfigBean mlmLookupConfigBean) throws Exception {
		AnvizentEncryptor anvizentEncryptor = new AnvizentEncryptor(mlmLookupConfigBean.getPrivateKey(), mlmLookupConfigBean.getIv());
		HashMap<String, String> headersMap = new HashMap<>();

		headersMap.put(MLM.CLIENT_TOKEN, anvizentEncryptor.encrypt(mlmLookupConfigBean.getClientId()));
		headersMap.put(MLM.USER_TOKEN, anvizentEncryptor.encrypt(mlmLookupConfigBean.getUserId()));

		return headersMap;
	}

	@SuppressWarnings("unchecked")
	public static void validateResponse(ResponseEntity<String> exchange, String url, String urlName) throws MLMResponseException {
		if (!RestUtil.isSuccessStatusCode(exchange)) {
			if (exchange.getBody() != null && !exchange.getBody().isEmpty()) {
				try {
					Object message = ((ArrayList<HashMap<String, Object>>) new JSONDeserializer<HashMap<String, Object>>().deserialize(exchange.getBody())
							.get("messages")).get(0).get("text");
					throw new MLMResponseException(
							"MLM " + urlName + " url '" + url + "' failed with message '" + message + "' and status '" + exchange.getStatusCode() + "'");
				} catch (Exception exception) {
					throw new MLMResponseException("MLM " + urlName + " url '" + url + "' failed with status '" + exchange.getStatusCode() + "'", exception);
				}
			} else {
				throw new MLMResponseException("MLM " + urlName + " url '" + url + "' failed with status '" + exchange.getStatusCode() + "'");
			}
		} else {
			if (exchange.getBody() == null || exchange.getBody().isEmpty()) {
				throw new MLMResponseException(
						"MLM " + urlName + " url '" + url + "' failed with status '" + exchange.getStatusCode() + "' and empty response");
			}
		}
	}
}
