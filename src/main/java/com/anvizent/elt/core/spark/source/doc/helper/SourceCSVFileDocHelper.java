package com.anvizent.elt.core.spark.source.doc.helper;

import com.anvizent.elt.core.spark.constant.ConfigConstants.File;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.ConfigConstants.Source.SourceCSV;
import com.anvizent.elt.core.spark.constant.HTMLTextStyle;
import com.anvizent.elt.core.spark.constant.HelpConstants.Type;
import com.anvizent.elt.core.spark.constant.MalformedRows;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.util.StringUtil;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class SourceCSVFileDocHelper extends DocHelper {

	public SourceCSVFileDocHelper(Factory factory) throws InvalidParameter {
		super(factory);
	}

	@Override
	public String[] getDescription() {
		return new String[] { "Loads CSV file(s) from the given path.",
				"This component will scan the entire input twice if '" + SourceCSV.HAS_HEADER
						+ " = false'. To avoid going through the entire data twice, disable `" + SourceCSV.INFER_SCHEMA
						+ "` option or specify the schema explicitly using `" + General.FIELDS + "` and `" + General.TYPES + "`." };
	}

	@Override
	public void addConfigDescriptions() throws InvalidParameter {
		configDescriptionUtil.addConfigDescription(File.PATH, General.YES, "", new String[] { "Path from where to read the CSV SourceCSV." });
		configDescriptionUtil.addConfigDescription(File.PART_FILES_PREFIX, General.NO, "", new String[] { "Prefix of part files." });
		configDescriptionUtil.addConfigDescription(SourceCSV.DELIMITER, General.NO, ",",
				new String[] { "Sets the single character as a separator for each field and value." });
		configDescriptionUtil.addConfigDescription(File.ENCODING, General.NO, "UTF-8", new String[] { "Decodes the CSV files by the given encoding type." });
		configDescriptionUtil.addConfigDescription(SourceCSV.QUOTE, General.NO, "\"",
				new String[] { "Sets the single character used for escaping quoted values where the separator can be part of the value." });
		configDescriptionUtil.addConfigDescription(File.ESCAPE_CHAR, General.NO, "\\",
				new String[] { "Sets the single character used for escaping quotes inside an already quoted value." });
		configDescriptionUtil.addConfigDescription(File.COMMENT, General.NO, "#",
				new String[] { "Sets the single character used for skipping lines beginning with this character." });
		configDescriptionUtil.addConfigDescription(SourceCSV.HAS_HEADER, General.NO, "false", new String[] { "Uses the first line as names of columns." }, "",
				"Boolean");
		configDescriptionUtil.addConfigDescription(SourceCSV.INFER_SCHEMA, General.NO, "false",
				new String[] { "Infers the input schema automatically from data. It requires one extra pass over the data, which is time consuming. "
						+ "Use this option only if you have very less data ot process time is not a factor." },
				"Boolean");
		configDescriptionUtil.addConfigDescription(SourceCSV.IGNORE_LEADING_WHITE_SPACE, General.NO, "false",
				new String[] { "A flag indicating whether or not leading whitespaces from values being read should be skipped." }, "Boolean");
		configDescriptionUtil.addConfigDescription(SourceCSV.IGNORE_TRAILING_WHITE_SPACE, General.NO, "false",
				new String[] { "A flag indicating whether or not trailing whitespaces from values being read should be skipped." }, "Boolean");
		configDescriptionUtil.addConfigDescription(SourceCSV.NULL_VALUE, General.NO, "empty string",
				new String[] { "Sets the string representation of a null value." });
		configDescriptionUtil.addConfigDescription(SourceCSV.MAX_COLUMNS, General.NO, "20480",
				new String[] { "Defines a hard limit of how many columns a record can have." });
		configDescriptionUtil.addConfigDescription(SourceCSV.MAX_CHARS_PER_COLUMN, General.NO, "-1", new String[] {
				"Defines the maximum number of characters allowed for any given value being read. By default, it is -1 meaning unlimited length." });
		configDescriptionUtil.addConfigDescription(SourceCSV.MALFORMED_ROWS, General.NO, MalformedRows.STOP_AND_FAIL.getValue(),
				new String[] { "`" + MalformedRows.IGNORE.getValue() + "`: ignores the whole corrupted records. `" + MalformedRows.STOP_AND_FAIL.getValue()
						+ "`: throws an exception when it meets corrupted records." });
		configDescriptionUtil.addConfigDescription(SourceCSV.MULTI_LINE, General.NO, "false",
				new String[] { "Parse one record, which may span multiple lines." }, "Boolean");
		configDescriptionUtil.addConfigDescription(General.FIELDS, "If types provided", "",
				new String[] { "Field names that are present in csv file Incase headers are not there in the file(s) or to specify schema" },
				"Number of " + General.FIELDS + " provided should be equals to number of " + General.TYPES + ".", Type.LIST_OF_STRINGS);
		configDescriptionUtil.addConfigDescription(General.TYPES, "If fields provided", "",
				StringUtil.join(new String[] { "Field types incase headers are not there in the file(s) or to specify schema. Types allowed are" },
						General.Type.ALLOWED_TYPES_AS_STRINGS),
				true, HTMLTextStyle.ORDERED_LIST, "Number of " + General.TYPES + " provided should be equals to " + General.FIELDS + ".", Type.LIST_OF_TYPES);
		configDescriptionUtil.addConfigDescription(File.DATE_FORMAT, "If types contain java.util.Date",
				new String[] { "Sets the string that indicates a date format. This applies to java.util.Date type." });
		configDescriptionUtil.addConfigDescription(General.DECIMAL_PRECISIONS, General.NO, String.valueOf(General.DECIMAL_PRECISION),
				new String[] { "Precisions for decimal type field." }, "", "Integer");
		configDescriptionUtil.addConfigDescription(General.DECIMAL_SCALES, General.NO, String.valueOf(General.DECIMAL_SCALE),
				new String[] { "Scales for decimal type field." }, "", "Integer");
	}
}