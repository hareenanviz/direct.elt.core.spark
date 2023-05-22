package com.anvizent.elt.core.spark;

import java.io.File;
import java.util.List;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class Test2 {

	public static void main(String[] args) {
		BasicAWSCredentials awsCreds = new BasicAWSCredentials("AKIAZPB5CU6Q4HJARPOT", "bfJeTN4vC8OOH6yqDP9WAebdTsblpAqeSGE2fxRA");
		AmazonS3 s3 = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(awsCreds)).build();

		List<Bucket> buckets = s3.listBuckets();
		System.out.println("Your {S3} buckets are:");
		for (Bucket b : buckets) {
			System.out.println("* " + b.getName());
		}

		String bucketName = "anvizdd";
		String fileObjKeyName = "datafiles_U1010701_M622_DW614_20220427121602844/datafiles_U1010701_M622_DW614_20220427121605125.csv";
		String fileName = "/Users/hareen.bejjanki/dev/anvizent.elt.workspace/AnvizentELTCoreLib/config files/tests_from_elt_team/tests from sanath/nano_seconds_date_format/datafiles_U1010701_M622_DW614_20220427121605125.csv";

		PutObjectRequest request = new PutObjectRequest(bucketName, fileObjKeyName, new File(fileName));
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentType("plain/text");
		metadata.addUserMetadata("title", "someTitle");
		request.setMetadata(metadata);
		s3.putObject(request);
	}

	public Test2() {

	}

}
