package com.anvizent.elt.core.spark;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;
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
import com.anvizent.elt.core.listener.ApplicationListener;
import com.anvizent.elt.core.spark.constant.Constants;
import com.anvizent.elt.core.spark.constant.FileFormat;
import com.anvizent.elt.core.spark.constant.SparkConstants;
import com.anvizent.elt.core.spark.constant.SparkConstants.Options;

/**
 * @author Hareen Bejjanki
 *
 */
public class S3CSVTest {

	public static void main(String[] args) throws IOException {
		Builder builder = SparkSession.builder().appName("test").config("spark.yarn.maxAppAttempts", 1).config("spark.task.maxFailures", 1)
		        .config("spark.extraListeners", ApplicationListener.class.getName()).master("local[*]");

		SparkSession sparkSession = builder.getOrCreate();

		sparkSession.sparkContext().hadoopConfiguration().set(SparkConstants.Config.S3_AWS_ACCESS_KEY_ID, "AKIAI4THPCF4DUIFQGFQ");
		sparkSession.sparkContext().hadoopConfiguration().set(SparkConstants.Config.S3_AWS_SECRET_ACCESS_KEY, "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");
		sparkSession.sparkContext().hadoopConfiguration().set(SparkConstants.Config.S3_IMPL, SparkConstants.S3.NATIVE_S3_FILE_SYSTEM);

		DataFrameReader dataFrameReader = sparkSession.read().option(Options.HEADER, "true");

		dataFrameReader = dataFrameReader.schema(getStructType());

		StructType structType = dataFrameReader.format(FileFormat.CSV.getValue())
		        .load(Constants.Protocol.S3 + "anvizdd/datafiles_U7_M5_IL8_DL29_20200322_010143_977_20200322_010145_08.csv").schema();

		for (StructField structField : structType.fields()) {
			System.out.println(structField.name() + " => " + structField.dataType());
		}

		AmazonS3Client s3Client = new AmazonS3Client(new BasicAWSCredentials("AKIAI4THPCF4DUIFQGFQ", "2sgGfhZbOuIVV2GzZvX5W33o61dEAZfbdLjibi+b"));

		ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName("anvizdd").withPrefix("datafiles_");
		ObjectListing objectListing = s3Client.listObjects(listObjectsRequest);

		System.out.println("\n\n\nList");

		for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
			System.out.println(objectSummary.getKey());
		}

		S3Object object = s3Client.getObject(new GetObjectRequest("anvizdd", "datafiles_U7_M5_IL8_DL29_20200322_010143_977_20200322_010145_08.csv"));
//		InputStream objectData = object.getObjectContent();
		InputStreamReader streamReader = new InputStreamReader(object.getObjectContent(), StandardCharsets.UTF_8);
		BufferedReader reader = new BufferedReader(streamReader);
		System.out.println(reader.readLine());
//		System.out.println((char) objectData.read());
//		System.out.println((char) objectData.read());
//		System.out.println((char) objectData.read());
//		System.out.println((char) objectData.read());
//		System.out.println((char) objectData.read());
//		System.out.println((char) objectData.read());

		System.out.println("test1234".matches("test*"));
	}

	private static StructType getStructType() {
		StructField[] structFields = new StructField[84];

		structFields[0] = DataTypes.createStructField("Invoice_Number", DataTypes.StringType, true);
		structFields[1] = DataTypes.createStructField("Register_Number", DataTypes.StringType, true);
		structFields[2] = DataTypes.createStructField("Summary_Line_Num", DataTypes.StringType, true);
		structFields[3] = DataTypes.createStructField("Invoice_Line_Number", DataTypes.StringType, true);
		structFields[4] = DataTypes.createStructField("Fiscal_Period", DataTypes.StringType, true);
		structFields[5] = DataTypes.createStructField("Fiscal_Year", DataTypes.StringType, true);
		structFields[6] = DataTypes.createStructField("Company_Id", DataTypes.StringType, true);
		structFields[7] = DataTypes.createStructField("Invoice_Type", DataTypes.StringType, true);
		structFields[8] = DataTypes.createStructField("Invoice_Date", DataTypes.StringType, true);
		structFields[9] = DataTypes.createStructField("Order_Number", DataTypes.StringType, true);
		structFields[10] = DataTypes.createStructField("Order_LineNumber", DataTypes.StringType, true);
		structFields[11] = DataTypes.createStructField("Order_Type", DataTypes.StringType, true);
		structFields[12] = DataTypes.createStructField("Plant_Id", DataTypes.StringType, true);
		structFields[13] = DataTypes.createStructField("Product_Id", DataTypes.StringType, true);
		structFields[14] = DataTypes.createStructField("Customer_ID", DataTypes.StringType, true);
		structFields[15] = DataTypes.createStructField("Order_Quantity", DataTypes.StringType, true);
		structFields[16] = DataTypes.createStructField("Warehouse_ID", DataTypes.StringType, true);
		structFields[17] = DataTypes.createStructField("Tax_Amount", DataTypes.StringType, true);
		structFields[18] = DataTypes.createStructField("Discount_Amount", DataTypes.StringType, true);
		structFields[19] = DataTypes.createStructField("Total_Amount", DataTypes.StringType, true);
		structFields[20] = DataTypes.createStructField("Sales_RespCode", DataTypes.StringType, true);
		structFields[21] = DataTypes.createStructField("Foreign_Curr_Invoice_Amount", DataTypes.StringType, true);
		structFields[22] = DataTypes.createStructField("Base_Currency_Code", DataTypes.StringType, true);
		structFields[23] = DataTypes.createStructField("Exchange_Rate", DataTypes.StringType, true);
		structFields[24] = DataTypes.createStructField("Transaction_Type", DataTypes.StringType, true);
		structFields[25] = DataTypes.createStructField("Journal_Number", DataTypes.StringType, true);
		structFields[26] = DataTypes.createStructField("Triang_Currency", DataTypes.StringType, true);
		structFields[27] = DataTypes.createStructField("Account_Conv_Rate", DataTypes.StringType, true);
		structFields[28] = DataTypes.createStructField("Triang_Conv_Rate", DataTypes.StringType, true);
		structFields[29] = DataTypes.createStructField("Post_Multi_Div_Flag", DataTypes.StringType, true);
		structFields[30] = DataTypes.createStructField("Account_Multi_Div_Flag", DataTypes.StringType, true);
		structFields[31] = DataTypes.createStructField("Triang_Multi1_Div_Flag", DataTypes.StringType, true);
		structFields[32] = DataTypes.createStructField("Product_Class", DataTypes.StringType, true);
		structFields[33] = DataTypes.createStructField("Customer_Class", DataTypes.StringType, true);
		structFields[34] = DataTypes.createStructField("Document_Type", DataTypes.StringType, true);
		structFields[35] = DataTypes.createStructField("Contract_Num", DataTypes.StringType, true);
		structFields[36] = DataTypes.createStructField("Bin_Id", DataTypes.StringType, true);
		structFields[37] = DataTypes.createStructField("Cust_PO_Number", DataTypes.StringType, true);
		structFields[38] = DataTypes.createStructField("Buying_Grp", DataTypes.StringType, true);
		structFields[39] = DataTypes.createStructField("Version", DataTypes.StringType, true);
		structFields[40] = DataTypes.createStructField("Releases", DataTypes.StringType, true);
		structFields[41] = DataTypes.createStructField("Nationality", DataTypes.StringType, true);
		structFields[42] = DataTypes.createStructField("Transaction_GL_Code", DataTypes.StringType, true);
		structFields[43] = DataTypes.createStructField("Cost_GL_Code", DataTypes.StringType, true);
		structFields[44] = DataTypes.createStructField("Tran_Analysis_Entry", DataTypes.StringType, true);
		structFields[45] = DataTypes.createStructField("Disc_Analysis_Entry", DataTypes.StringType, true);
		structFields[46] = DataTypes.createStructField("Cost_Analysis_Entry", DataTypes.StringType, true);
		structFields[47] = DataTypes.createStructField("Warehouse_Account", DataTypes.StringType, true);
		structFields[48] = DataTypes.createStructField("Warehouse_Analysis_Entry", DataTypes.StringType, true);
		structFields[49] = DataTypes.createStructField("Tax_Account", DataTypes.StringType, true);
		structFields[50] = DataTypes.createStructField("Mass", DataTypes.StringType, true);
		structFields[51] = DataTypes.createStructField("Volume", DataTypes.StringType, true);
		structFields[52] = DataTypes.createStructField("Cost_Value", DataTypes.StringType, true);
		structFields[53] = DataTypes.createStructField("Federal_Tax_Value", DataTypes.StringType, true);
		structFields[54] = DataTypes.createStructField("Invoice_Line_Discount_Amt", DataTypes.StringType, true);
		structFields[55] = DataTypes.createStructField("Tax_Value", DataTypes.StringType, true);
		structFields[56] = DataTypes.createStructField("Post_Value", DataTypes.StringType, true);
		structFields[57] = DataTypes.createStructField("Discount_GL_Code", DataTypes.StringType, true);
		structFields[58] = DataTypes.createStructField("Warehouse_Amount", DataTypes.StringType, true);
		structFields[59] = DataTypes.createStructField("GL_Period", DataTypes.StringType, true);
		structFields[60] = DataTypes.createStructField("GL_Year", DataTypes.StringType, true);
		structFields[61] = DataTypes.createStructField("Ledger_Year", DataTypes.StringType, true);
		structFields[62] = DataTypes.createStructField("Ledger_Period", DataTypes.StringType, true);
		structFields[63] = DataTypes.createStructField("Price_Code", DataTypes.StringType, true);
		structFields[64] = DataTypes.createStructField("Area", DataTypes.StringType, true);
		structFields[65] = DataTypes.createStructField("Tax_Code", DataTypes.StringType, true);
		structFields[66] = DataTypes.createStructField("Tax_Status", DataTypes.StringType, true);
		structFields[67] = DataTypes.createStructField("History_Reqd", DataTypes.StringType, true);
		structFields[68] = DataTypes.createStructField("Federal_Tax_Code", DataTypes.StringType, true);
		structFields[69] = DataTypes.createStructField("User_Field1", DataTypes.StringType, true);
		structFields[70] = DataTypes.createStructField("User_Field2", DataTypes.StringType, true);
		structFields[71] = DataTypes.createStructField("Inter_Branch_Trf_Flag", DataTypes.StringType, true);
		structFields[72] = DataTypes.createStructField("Prod_Class_Upd_Flag", DataTypes.StringType, true);
		structFields[73] = DataTypes.createStructField("GL_Distr_Upd", DataTypes.StringType, true);
		structFields[74] = DataTypes.createStructField("Salesperson_Upd", DataTypes.StringType, true);
		structFields[75] = DataTypes.createStructField("SalesGL_Int_Reqd", DataTypes.StringType, true);
		structFields[76] = DataTypes.createStructField("Stock_Upd", DataTypes.StringType, true);
		structFields[77] = DataTypes.createStructField("Interface_Flag", DataTypes.StringType, true);
		structFields[78] = DataTypes.createStructField("Sales_Turn_Oprted", DataTypes.StringType, true);
		structFields[79] = DataTypes.createStructField("Abc_Upd", DataTypes.StringType, true);
		structFields[80] = DataTypes.createStructField("Eec_Invoice_Flag", DataTypes.StringType, true);
		structFields[81] = DataTypes.createStructField("GL_Int_Updated", DataTypes.StringType, true);
		structFields[82] = DataTypes.createStructField("GL_Int_Printed", DataTypes.StringType, true);
		structFields[83] = DataTypes.createStructField("Time_Stamp", DataTypes.StringType, true);

		return DataTypes.createStructType(structFields);
	}

}
