package com.anvizent.elt.core.spark.sink.doc.helper;

import org.apache.spark.sql.SaveMode;

import com.anvizent.elt.core.spark.constant.Compression;
import com.anvizent.elt.core.spark.constant.ConfigConstants.File;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.HTMLTextStyle;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.factory.Factory;

/**
 * @author Hareen Bejjanki
 *
 */
public class JSONFileSinkDocHelper extends DocHelper {

	public JSONFileSinkDocHelper(Factory factory) throws InvalidParameter {
		super(factory);
		// TODO Auto-generated constructor stub
	}

	@Override
	public String[] getDescription() {
		return new String[] { "Write the data as JSON flat file(s) in the given path, with give configuration." };
	}

	@Override
	public void addConfigDescriptions() throws InvalidParameter {
		configDescriptionUtil.addConfigDescription(File.PATH, General.YES, "", new String[] { "Where JSON file needs to be saved." });
		configDescriptionUtil.addConfigDescription(File.SINGLE_FILE, General.NO, "false", new String[] { "Save as single file or part files." }, "", "Boolean");

		configDescriptionUtil
		        .addConfigDescription(
		                File.SAVE_MODE, General.NO, SaveMode.ErrorIfExists.name(), new String[] { "Below are the save modes that are allowed",
		                        SaveMode.Append.name(), SaveMode.ErrorIfExists.name(), SaveMode.Ignore.name(), SaveMode.Overwrite.name() },
		                true, HTMLTextStyle.ORDERED_LIST);
		configDescriptionUtil.addConfigDescription(File.TIME_ZONE, General.NO, "",
		        new String[] {
		                "Sets the string that indicates a time zone ID to be used to format timestamps in "
		                        + "the JSON datasources or partition values. The following formats of timeZone are supported:",
		                "Region-based zone ID: It should have the form 'area/city', such as 'America/Los_Angeles'.",
		                "Zone offset: It should be in the format '(+|-)HH:mm', for example '-08:00' or '+01:00'. "
		                        + "Also 'UTC' and 'Z' are supported as aliases of '+00:00'.",
		                "Other short names like 'CST' are not recommended to use because they can be ambiguous." },
		        true, HTMLTextStyle.ORDERED_LIST);
		configDescriptionUtil.addConfigDescription(File.DATE_FORMAT, General.NO, "yyyy-MM-dd",
		        new String[] { "Specify the date format which is to be used to write the date fields." });
		configDescriptionUtil.addConfigDescription(File.TIMESTAMP_FORMAT, General.NO, "yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]",
		        new String[] { "Specify the date and time format which is to be used to write the timestamp fields." });
		configDescriptionUtil.addConfigDescription(File.ENCODING, General.NO, "UTF-8",
		        new String[] { "For reading, allows to forcibly set one of standard basic or extended encoding for the JSON files. "
		                + "For example UTF-16BE, UTF-32LE. For writing, Specifies encoding (charset) of saved json files." });
		configDescriptionUtil.addConfigDescription(File.LINE_SEP, General.NO, "\\n",
		        new String[] { "Below are the compressions allowed", "\\r", "\\r\\n", "\\n" }, true, HTMLTextStyle.ORDERED_LIST);
		configDescriptionUtil.addConfigDescription(
		        File.COMPRESSION, General.YES, "", new String[] { "Below are the compressions allowed", Compression.NONE.name(), Compression.BZIP2.name(),
		                Compression.GZIP.name(), Compression.LZ4.name(), Compression.SNAPPY.name(), Compression.DEFLATE.name() },
		        true, HTMLTextStyle.ORDERED_LIST);
		configDescriptionUtil.addConfigDescription(File.IGNORE_NULL_FIELDS, General.NO, "",
		        new String[] { "Whether to ignore null fields when generating JSON objects." });
	}

}
