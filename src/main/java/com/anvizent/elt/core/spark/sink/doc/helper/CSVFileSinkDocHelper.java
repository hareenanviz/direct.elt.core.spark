package com.anvizent.elt.core.spark.sink.doc.helper;

import org.apache.spark.sql.SaveMode;

import com.anvizent.elt.core.spark.constant.Compression;
import com.anvizent.elt.core.spark.constant.ConfigConstants.File;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Sink.CSVSink;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Source.SourceCSV;
import com.anvizent.elt.core.spark.constant.HTMLTextStyle;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.factory.Factory;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class CSVFileSinkDocHelper extends DocHelper {

	public CSVFileSinkDocHelper(Factory factory) throws InvalidParameter {
		super(factory);
		// TODO Auto-generated constructor stub
	}

	@Override
	public String[] getDescription() {
		return new String[] { "Write the data as CSV file(s) in the given path, with give configuration." };
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
		configDescriptionUtil.addConfigDescription(
		        File.COMPRESSION, General.YES, "", new String[] { "Below are the compressions allowed", Compression.NONE.name(), Compression.BZIP2.name(),
		                Compression.GZIP.name(), Compression.LZ4.name(), Compression.SNAPPY.name(), Compression.DEFLATE.name() },
		        true, HTMLTextStyle.ORDERED_LIST);
		configDescriptionUtil.addConfigDescription(SourceCSV.DELIMITER, General.NO, ",",
		        new String[] { "Sets the single character as a separator for each field and value." });
		configDescriptionUtil.addConfigDescription(SourceCSV.QUOTE, General.NO, "\"",
		        new String[] { "Sets the single character used for escaping quoted values where the separator can be part of the value." });
		configDescriptionUtil.addConfigDescription(CSVSink.QUOTE_ALL, General.NO, "false",
		        new String[] { "Sets all the character used for escaping quoted values where the separator can be part of the value." }, "", "Boolean");
		configDescriptionUtil.addConfigDescription(File.ESCAPE_CHAR, General.NO, "\\",
		        new String[] { "Sets the single character used for escaping quotes inside an already quoted value." });
		configDescriptionUtil.addConfigDescription(CSVSink.ESCAPE_QUOTES, General.NO, "true", new String[] {
		        "A flag indicating whether values containing quotes should always be enclosed in quotes. Default is to escape all values containing a quote character" },
		        "", "Boolean");
		configDescriptionUtil.addConfigDescription(File.ENCODING, General.NO, "UTF-8",
		        new String[] { "For reading, allows to forcibly set one of standard basic or extended encoding for the JSON files. "
		                + "For example UTF-16BE, UTF-32LE. For writing, Specifies encoding (charset) of saved json files." });
		configDescriptionUtil.addConfigDescription(File.LINE_SEP, General.NO, "\\n",
		        new String[] { "Below are the compressions allowed", "\\r", "\\r\\n", "\\n" }, true, HTMLTextStyle.ORDERED_LIST);
		configDescriptionUtil.addConfigDescription(File.CHAR_TO_ESCAPE_QUOTE_ESCAPING, General.NO, "value specified for " + File.ESCAPE_CHAR + " or \\0",
		        new String[] { "Sets the string representation of a empty value." });
		configDescriptionUtil.addConfigDescription(File.EMPTY_VALUE, General.NO, "<empty string>",
		        new String[] { "Sets the string representation of a empty value." });

		configDescriptionUtil.addConfigDescription(SourceCSV.HAS_HEADER, General.NO, "false", new String[] { "Uses the first line as names of columns." }, "",
		        "Boolean");
		configDescriptionUtil.addConfigDescription(SourceCSV.NULL_VALUE, General.NO, "<empty string>",
		        new String[] { "Sets the string representation of a null value." });
		configDescriptionUtil.addConfigDescription(SourceCSV.IGNORE_LEADING_WHITE_SPACE, General.NO, "false",
		        new String[] { "A flag indicating whether or not leading whitespaces from values being read should be skipped." }, "", "Boolean");
		configDescriptionUtil.addConfigDescription(SourceCSV.IGNORE_TRAILING_WHITE_SPACE, General.NO, "false",
		        new String[] { "A flag indicating whether or not trailing whitespaces from values being read should be skipped." }, "", "Boolean");
		configDescriptionUtil.addConfigDescription(File.DATE_FORMAT, General.NO, "",
		        new String[] { "Specify the date format which is to be used to write the date fields." });
		configDescriptionUtil.addConfigDescription(File.TIMESTAMP_FORMAT, General.NO, "",
		        new String[] { "Specify the timestamp format which is to be used to write the date fields." });

	}

}
