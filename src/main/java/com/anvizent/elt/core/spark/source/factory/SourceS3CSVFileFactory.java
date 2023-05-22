package com.anvizent.elt.core.spark.source.factory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.lib.exception.ImproperValidationException;
import com.anvizent.elt.core.lib.exception.UnsupportedException;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.constant.StatsType;
import com.anvizent.elt.core.spark.config.bean.S3Connection;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Source;
import com.anvizent.elt.core.spark.constant.FileFormat;
import com.anvizent.elt.core.spark.constant.SparkConstants;
import com.anvizent.elt.core.spark.constant.SparkConstants.Options;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.schema.validator.SchemaValidator;
import com.anvizent.elt.core.spark.source.config.bean.SourceS3CSVFileConfigBean;
import com.anvizent.elt.core.spark.source.doc.helper.SourceS3CSVFileDocHelper;
import com.anvizent.elt.core.spark.source.validator.SourceS3CSVFileValidator;
import com.anvizent.elt.core.spark.validator.ResourceValidator;
import com.anvizent.elt.core.spark.validator.Validator;

/**
 * @author Hareen Bejjanki
 *
 */
public class SourceS3CSVFileFactory extends SQLSourceFactory {

	private static final long serialVersionUID = 1L;

	@Override
	public Dataset<Row> read(ConfigBean configBean) throws ImproperValidationException, UnsupportedException, IOException {
		SourceS3CSVFileConfigBean sourceS3CSVFileConfigBean = (SourceS3CSVFileConfigBean) configBean;
		S3Connection s3Connection = sourceS3CSVFileConfigBean.getS3Connection();

		ApplicationBean.getInstance().getSparkSession().sparkContext().hadoopConfiguration().set(SparkConstants.Config.S3_AWS_ACCESS_KEY_ID,
		        s3Connection.getAccessKey());
		ApplicationBean.getInstance().getSparkSession().sparkContext().hadoopConfiguration().set(SparkConstants.Config.S3_AWS_SECRET_ACCESS_KEY,
		        s3Connection.getSecretKey());
		ApplicationBean.getInstance().getSparkSession().sparkContext().hadoopConfiguration().set(SparkConstants.Config.S3_IMPL,
		        SparkConstants.S3.NATIVE_S3_FILE_SYSTEM);

		DataFrameReader dataFrameReader = ApplicationBean.getInstance().getSparkSession().read();

		if (sourceS3CSVFileConfigBean.getOptions() != null && !sourceS3CSVFileConfigBean.getOptions().isEmpty()) {
			dataFrameReader = dataFrameReader.options(sourceS3CSVFileConfigBean.getOptions());
		}
		if (sourceS3CSVFileConfigBean.getStructType() != null) {
			dataFrameReader = dataFrameReader.schema(sourceS3CSVFileConfigBean.getStructType());
		} else if (sourceS3CSVFileConfigBean.getOptions().containsKey(Options.HEADER)
		        && sourceS3CSVFileConfigBean.getOptions().get(Options.HEADER).equalsIgnoreCase("true")) {
			dataFrameReader = dataFrameReader.schema(getStructTypeFromS3File(s3Connection, sourceS3CSVFileConfigBean));
		}

		return dataFrameReader.format(FileFormat.CSV.getValue()).load(sourceS3CSVFileConfigBean.getPath());
	}

	@SuppressWarnings("deprecation")
	private StructType getStructTypeFromS3File(S3Connection s3Connection, SourceS3CSVFileConfigBean sourceS3CSVFileConfigBean) throws IOException {
		AmazonS3Client s3Client = new AmazonS3Client(new BasicAWSCredentials(s3Connection.getAccessKey(), s3Connection.getSecretKey()));
		String prefix = sourceS3CSVFileConfigBean.getRawPath();

		if (prefix.charAt(0) == '/' || prefix.charAt(0) == '\\') {
			prefix = prefix.substring(1);
		}

		int index = getPrefixIndex(prefix);
		ListObjectsRequest listObjectsRequest = getListObjectsRequest(s3Connection, prefix, index);

		String filePath = getS3FilePath(s3Client, listObjectsRequest, s3Connection.getBucketName(), sourceS3CSVFileConfigBean.getPath(), index);
		String header = getHeader(s3Client, s3Connection, filePath);

		return getStructType(header);
	}

	private StructType getStructType(String header) throws IOException {
		CSVRecord csvRecord = getHeaderCSV(header);
		StructField structFields[] = new StructField[csvRecord.size()];

		for (int i = 0; i < structFields.length; i++) {
			structFields[i] = DataTypes.createStructField(csvRecord.get(i), DataTypes.StringType, true);
		}

		return new StructType(structFields);
	}

	private CSVRecord getHeaderCSV(String header) throws IOException {
		CSVFormat format = CSVFormat.RFC4180.withDelimiter(',').withQuote('"');
		CSVParser csvParser = CSVParser.parse(header, format);
		return csvParser.getRecords().get(0);
	}

	private ListObjectsRequest getListObjectsRequest(S3Connection s3Connection, String prefix, int index) {
		ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(s3Connection.getBucketName());

		if (index > 0) {
			listObjectsRequest.withPrefix(prefix.substring(index));
		} else if (index < 0) {
			listObjectsRequest.withPrefix(prefix);
		}

		return listObjectsRequest;
	}

	private String getHeader(AmazonS3Client s3Client, S3Connection s3Connection, String filePath) throws IOException {
		S3Object object = s3Client.getObject(new GetObjectRequest(s3Connection.getBucketName(), filePath));
		InputStreamReader streamReader = new InputStreamReader(object.getObjectContent(), StandardCharsets.UTF_8);
		BufferedReader reader = new BufferedReader(streamReader);
		return reader.readLine();
	}

	private int getPrefixIndex(String prefix) {
		char[] charArray = prefix.toCharArray();
		boolean escape = false;

		for (int i = 0; i < charArray.length; i++) {
			if (charArray[i] == '\\') {
				escape = !escape;
			} else if (charArray[i] == '*') {
				if (escape) {
					escape = false;
				} else {
					return i;
				}
			} else if (charArray[i] == '?') {
				if (escape) {
					escape = false;
				} else {
					return i;
				}
			}
		}

		return -1;
	}

	private String getS3FilePath(AmazonS3Client s3Client, ListObjectsRequest listObjectsRequest, String bucketName, String path, int index)
	        throws FileNotFoundException {
		ObjectListing objectListing = s3Client.listObjects(listObjectsRequest);
		if (objectListing != null) {
			List<S3ObjectSummary> s3ObjectSummaries = objectListing.getObjectSummaries();
			if (s3ObjectSummaries == null || s3ObjectSummaries.isEmpty()) {
				throw new FileNotFoundException("S3 bucket with file '" + bucketName + "/" + path + "' not found!");
			} else {
				if (index < 0) {
					return s3ObjectSummaries.get(0).getKey();
				} else {
					String regex = getFileRegex(path);
					for (S3ObjectSummary objectSummary : s3ObjectSummaries) {
						if (objectSummary.getKey().matches(regex)) {
							return objectSummary.getKey();
						}
					}
				}
			}
		} else {
			throw new FileNotFoundException("S3 bucket with file '" + bucketName + "/" + path + "' not found!");
		}

		throw new FileNotFoundException("S3 bucket with file '" + bucketName + "/" + path + "' not found!");
	}

	private String getFileRegex(String glob) {
		String out = "^";
		for (int i = 0; i < glob.length(); ++i) {
			final char c = glob.charAt(i);
			switch (c) {
				case '*':
					out += ".*";
					break;
				case '?':
					out += '.';
					break;
				case '.':
					out += "\\.";
					break;
				case '\\':
					out += "\\\\";
					break;
				default:
					out += c;
			}
		}
		out += '$';

		return out;
	}

	@Override
	public DocHelper getDocHelper() throws InvalidParameter {
		return new SourceS3CSVFileDocHelper(this);
	}

	@Override
	public String getName() {
		return Source.Components.SOURCE_S3_CSV.get(General.NAME);
	}

	@Override
	public Validator getValidator() {
		return new SourceS3CSVFileValidator(this);
	}

	@Override
	public SchemaValidator getSchemaValidator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Integer getMaxInputs() {
		return 0;
	}

	@Override
	public Integer getMinInputs() {
		return 0;
	}

	@Override
	protected void createSpecialAccumulators(ConfigBean configBean, StatsType statsType, boolean componentLevel) {
		// TODO Auto-generated method stub
	}

	@Override
	public ResourceValidator getResourceConfigValidator() {
		// TODO Auto-generated method stub
		return null;
	}
}
