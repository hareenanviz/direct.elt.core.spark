package com.anvizent.elt.core.spark.sink.doc.helper;

import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General.S3;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.factory.Factory;

/**
 * @author Hareen Bejjanki
 *
 */
public class S3JSONFileSinkDocHelper extends JSONFileSinkDocHelper {

	public S3JSONFileSinkDocHelper(Factory factory) throws InvalidParameter {
		super(factory);
	}

	@Override
	public String[] getDescription() {
		return new String[] { "Write the data as JSON flat file(s) into the given S3 bucket in the given path, with give configuration." };
	}

	@Override
	public void addConfigDescriptions() throws InvalidParameter {
		super.addConfigDescriptions();
		configDescriptionUtil.addConfigDescription(S3.ACCESS_KEY, General.YES, "", new String[] { "Access key provided by aws S3 to connect given bucket." });
		configDescriptionUtil.addConfigDescription(S3.SECRET_KEY, General.YES, "", new String[] { "Secret key provided by aws S3 to connect given bucket." });
		configDescriptionUtil.addConfigDescription(S3.BUCKET_NAME, General.YES, "", new String[] { "Bucket key provided by aws S3 to connect given bucket." });
	}

}
