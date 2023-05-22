package com.anvizent.elt.core.spark.config.util;

import java.awt.Desktop;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import com.anvizent.elt.core.spark.config.ComponentConfigDescriptionDoc;
import com.anvizent.elt.core.spark.config.ComponentConfigDoc;
import com.anvizent.elt.core.spark.constant.ConfigConstants.General;
import com.anvizent.elt.core.spark.constant.Constants;
import com.anvizent.elt.core.spark.constant.HTMLTextStyle;
import com.anvizent.elt.core.spark.constant.HelpConstants;
import com.anvizent.elt.core.spark.constant.HelpConstants.Tags;
import com.anvizent.elt.core.spark.constant.Mapping;
import com.anvizent.elt.core.spark.doc.helper.DocHelper;
import com.anvizent.elt.core.spark.exception.InvalidParameter;
import com.anvizent.elt.core.spark.factory.Factory;
import com.anvizent.elt.core.spark.mapping.doc.helper.MappingDocHelper;
import com.anvizent.elt.core.spark.util.IndexedAndLinkedMap;
import com.anvizent.elt.core.spark.util.StringUtil;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class ProcessHTMLHelp {

	public static void process(LinkedHashMap<String, LinkedHashMap<String, Factory>> factories, File parentDirectory, boolean reProcess)
	        throws IOException, InvalidParameter {
		if (parentDirectory == null || !parentDirectory.exists() || !parentDirectory.isDirectory()) {
			parentDirectory = Constants.CURRENT_DIRECTORY;
		}

		if (reProcess) {
			BufferedWriter writer = getHTMLPage(parentDirectory);
			generateHTML(factories, writer);
			writer.flush();
			writer.close();
		}

		Desktop.getDesktop().open(new File(parentDirectory, HelpConstants.General.HTML_PAGE_PATH));
	}

	private static void generateHTML(LinkedHashMap<String, LinkedHashMap<String, Factory>> factories, BufferedWriter writer)
	        throws IOException, InvalidParameter {
		writeTag(writer, Tags.Begin.HTML, 0);
		writeHead(writer);
		writeBody(factories, writer);
		writeTag(writer, Tags.End.HTML, 0);
	}

	private static void writeTag(BufferedWriter writer, String tag, int i) throws IOException {
		writeTag(writer, tag, i, true);
	}

	private static void writeTag(BufferedWriter writer, String tag, int i, boolean addNewLine) throws IOException {
		writer.write(StringUtil.nTimes(HelpConstants.General.TAB, i));
		writer.write(tag);
		if (addNewLine) {
			writer.write(HelpConstants.General.NEW_LINE);
		}
	}

	private static void writeText(BufferedWriter writer, String text, boolean addNewLine) throws IOException {
		writer.write(text);
		if (addNewLine) {
			writer.write(HelpConstants.General.NEW_LINE);
		}
	}

	private static void writeBody(LinkedHashMap<String, LinkedHashMap<String, Factory>> factories, BufferedWriter writer) throws IOException, InvalidParameter {
		writeTag(writer, Tags.Begin.BODY, 1);
		int depth = 2;

		writeMainTabs(writer, depth);

		writeTag(writer, Tags.Begin.get(Tags.Begin.DIV, Tags.Attribute.Key.CLASS, Tags.Attribute.Value.TAB_CONTENT), depth);
		writeTag(writer, StringUtil.nTimes(Tags.Begin.BREAK, 1), depth + 1);

		writerComponents(HelpConstants.General.SOURCES_TITLE, factories.get(General.SOURCE), writer, true, depth + 1);
		writerComponents(HelpConstants.General.OPERATIONS_TITLE, factories.get(General.OPERATION), writer, false, depth + 1);
		writerComponents(HelpConstants.General.FILTERS_TITLE, factories.get(General.FILTER), writer, false, depth + 1);
		writerComponents(HelpConstants.General.SINKS_TITLE, factories.get(General.SINK), writer, false, depth + 1);
		writerMappingComponentsMainTable(writer, depth + 1);

		writeTag(writer, Tags.End.DIV, depth);

		writeTag(writer, Tags.End.BODY, 1);
	}

	private static void writeMainTabs(BufferedWriter writer, int depth) throws IOException {
		writeTag(writer, Tags.Begin.get(Tags.Begin.DIV, Tags.Attribute.Key.CLASS, Tags.Attribute.Value.CONTAINER), depth);
		writeTabs(writer, depth + 1, HelpConstants.General.SOURCES_TITLE, HelpConstants.General.SOURCES_TITLE.replace(' ', '_'),
		        HelpConstants.General.OPERATIONS_TITLE, HelpConstants.General.OPERATIONS_TITLE.replace(' ', '_'), HelpConstants.General.FILTERS_TITLE,
		        HelpConstants.General.FILTERS_TITLE.replace(' ', '_'), HelpConstants.General.SINKS_TITLE, HelpConstants.General.SINKS_TITLE.replace(' ', '_'),
		        HelpConstants.General.MAPPING_OPERATIONS_TITLE, HelpConstants.General.MAPPING_OPERATIONS_TITLE.replace(' ', '_'));
		writeTag(writer, Tags.End.DIV, depth);
	}

	private static void writeTabs(BufferedWriter writer, int depth, String... titlesAndIds) throws IOException {
		writeTag(writer,
		        Tags.Begin.get(Tags.Begin.UL, Tags.Attribute.Key.CLASS, Tags.Attribute.Value.TABS, Tags.Attribute.Key.ROLE, Tags.Attribute.Value.TABS_LIST),
		        depth);

		for (int i = 0; i < titlesAndIds.length; i++) {
			writeTab(writer, titlesAndIds[i++], titlesAndIds[i], i == 1, depth + 1);
		}

		writeTag(writer, Tags.End.UL, depth);
	}

	private static void writeTab(BufferedWriter writer, String title, String id, boolean active, int depth) throws IOException {
		writeTag(writer, Tags.Begin.get(Tags.Begin.LI, Tags.Attribute.Key.CLASS, Tags.Attribute.Value.TAB_ITEM), depth);

		if (active) {
			writeTag(writer, Tags.Begin.get(Tags.Begin.A, Tags.Attribute.Key.CLASS, Tags.Attribute.Value.ACTIVE_TAB_LINK, Tags.Attribute.Key.DATA_TOGGLE,
			        Tags.Attribute.Value.TAB, Tags.Attribute.Key.HREF, "#" + id), depth, false);
		} else {
			writeTag(writer, Tags.Begin.get(Tags.Begin.A, Tags.Attribute.Key.CLASS, Tags.Attribute.Value.NORMAL_TAB_LINK, Tags.Attribute.Key.DATA_TOGGLE,
			        Tags.Attribute.Value.TAB, Tags.Attribute.Key.HREF, "#" + id), depth, false);
		}

		writeText(writer, title, false);
		writeText(writer, Tags.End.A, true);

		writeTag(writer, Tags.End.LI, depth);
	}

	private static void writerMappingComponentsMainTable(BufferedWriter writer, int depth) throws IOException, InvalidParameter {
		writeTag(writer, Tags.Begin.get(Tags.Begin.DIV, Tags.Attribute.Key.ID, HelpConstants.General.MAPPING_OPERATIONS_TITLE.replace(' ', '_'),
		        Tags.Attribute.Key.CLASS, Tags.Attribute.Value.NORMAL_TAB_CONTAINER), depth);

		String[] titlesAndIds = new String[Mapping.values().length * 2];
		int i = 0;

		for (Mapping factory : Mapping.values()) {
			titlesAndIds[i++] = factory.getMappingName();
			titlesAndIds[i++] = factory.name();
		}

		writeSubTabs(writer, titlesAndIds, depth + 1);

		writeTag(writer, Tags.Begin.get(Tags.Begin.DIV, Tags.Attribute.Key.CLASS, Tags.Attribute.Value.TAB_CONTENT), depth + 1);
		writerMappingComponents(writer, depth + 2);
		writeTag(writer, Tags.End.DIV, depth + 1);

		writeTag(writer, Tags.End.DIV, depth);
	}

	private static void writerComponents(String title, LinkedHashMap<String, Factory> factories, BufferedWriter writer, boolean active, int depth)
	        throws IOException, InvalidParameter {
		if (active) {
			writeTag(writer, Tags.Begin.get(Tags.Begin.DIV, Tags.Attribute.Key.ID, title, Tags.Attribute.Key.CLASS, Tags.Attribute.Value.ACTIVE_TAB_CONTAINER),
			        depth);
		} else {
			writeTag(writer, Tags.Begin.get(Tags.Begin.DIV, Tags.Attribute.Key.ID, title, Tags.Attribute.Key.CLASS, Tags.Attribute.Value.NORMAL_TAB_CONTAINER),
			        depth);
		}

		String[] titlesAndIds = new String[factories.size() * 2];
		int i = 0;

		for (Entry<String, Factory> factory : factories.entrySet()) {
			titlesAndIds[i++] = factory.getValue().getName();
			titlesAndIds[i++] = factory.getKey();
		}

		writeSubTabs(writer, titlesAndIds, depth + 1);

		writeTag(writer, Tags.Begin.get(Tags.Begin.DIV, Tags.Attribute.Key.CLASS, Tags.Attribute.Value.TAB_CONTENT), depth + 1);

		writerComponents(factories, writer, depth + 2);

		writeTag(writer, Tags.End.DIV, depth + 1);

		writeTag(writer, Tags.End.DIV, depth);
	}

	private static void writeSubTabs(BufferedWriter writer, String[] titlesAndIds, int depth) throws IOException {
		writeTag(writer, Tags.Begin.get(Tags.Begin.DIV, Tags.Attribute.Key.CLASS, Tags.Attribute.Value.CONTAINER), depth);
		writeTabs(writer, depth + 1, titlesAndIds);
		writeTag(writer, Tags.End.DIV, depth);
	}

	private static void writerMappingComponents(BufferedWriter writer, int depth) throws IOException, InvalidParameter {
		String tabType = Tags.Attribute.Value.ACTIVE_TAB_CONTAINER;

		for (Mapping mapping : Mapping.values()) {
			writeTag(writer, Tags.Begin.get(Tags.Begin.DIV, Tags.Attribute.Key.ID, mapping.name(), Tags.Attribute.Key.CLASS, tabType), depth);

			writeTag(writer, Tags.Begin.TABLE_ROW, depth);
			writeTag(writer, Tags.Begin.TABLE_DATA, depth + 1);
			writerMappingComponent(mapping, writer, depth + 2);
			writeTag(writer, Tags.End.TABLE_DATA, depth + 1);
			writeTag(writer, Tags.End.TABLE_ROW, depth);

			writeTag(writer, Tags.End.DIV, depth);

			tabType = Tags.Attribute.Value.NORMAL_TAB_CONTAINER;
		}
	}

	private static void writerComponents(LinkedHashMap<String, Factory> factories, BufferedWriter writer, int depth) throws IOException, InvalidParameter {
		if (factories != null) {
			String tabType = Tags.Attribute.Value.ACTIVE_TAB_CONTAINER;
			for (Entry<String, Factory> factory : factories.entrySet()) {
				writeTag(writer, Tags.Begin.get(Tags.Begin.DIV, Tags.Attribute.Key.ID, factory.getKey(), Tags.Attribute.Key.CLASS, tabType), depth);

				writerComponent(factory, writer, depth + 3);

				writeTag(writer, Tags.End.DIV, depth);

				tabType = Tags.Attribute.Value.NORMAL_TAB_CONTAINER;
			}
		}
	}

	private static void writerMappingComponent(Mapping mapping, BufferedWriter writer, int depth) throws IOException, InvalidParameter {
		writeTag(writer, Tags.Begin.COMPONENT_TABLE, depth);

		writeTag(writer, Tags.Begin.TABLE_BODY, depth + 1);

		MappingDocHelper docHelper = mapping.getMappingFactory().getDocHelper();

		writeSemiTHeadRow(writer, depth + 2, "Config Name: ", mapping.getMappingName(), "Factory name not specified by dev team!");
		writeSemiTHeadRow(writer, depth + 2, "Description: ", getDescription(mapping.getDescription()), "Factory description not specified by dev team!");

		writeTag(writer, Tags.Begin.TABLE_ROW, depth + 2);
		writeTHeadData(writer, depth + 3, "Configs: ", "COnfigs: ");
		writeTag(writer, Tags.Begin.TABLE_DATA, depth + 3);
		if (docHelper != null) {
			writeConfigs(writer, depth + 4, docHelper.getConfigDescriptions());
		}
		writeTag(writer, Tags.End.TABLE_DATA, depth + 3);
		writeTag(writer, Tags.End.TABLE_ROW, depth + 2);

		writeTag(writer, Tags.End.TABLE_BODY, depth + 1);

		writeTag(writer, Tags.End.TABLE, depth);
	}

	private static void writerComponent(Entry<String, Factory> factoryEntry, BufferedWriter writer, int depth) throws IOException, InvalidParameter {
		writeTag(writer, Tags.Begin.COMPONENT_TABLE, depth);

		writeTag(writer, Tags.Begin.TABLE_BODY, depth + 1);

		DocHelper docHelper = factoryEntry.getValue().getDocHelper();

		writeSemiTHeadRow(writer, depth + 2, "Config Name: ", factoryEntry.getKey(), "Factory name not specified by dev team!");
		writeSemiTHeadRow(writer, depth + 2, "Description: ", getDescription(docHelper.getDescription()), "Factory description not specified by dev team!");

		writeTag(writer, Tags.Begin.TABLE_ROW, depth + 2);
		writeTHeadData(writer, depth + 3, "Configs: ", "COnfigs: ");
		writeTag(writer, Tags.Begin.TABLE_DATA, depth + 3);
		writeConfigs(writer, depth + 4, docHelper.getConfigDescriptions());
		writeTag(writer, Tags.End.TABLE_DATA, depth + 3);
		writeTag(writer, Tags.End.TABLE_ROW, depth + 2);

		writeTag(writer, Tags.End.TABLE_BODY, depth + 1);

		writeTag(writer, Tags.End.TABLE, depth);
	}

	private static String getDescription(String[] description) {
		if (description != null && description.length > 0) {
			return String.join("<br />", description);
		} else {
			return "No description specified by dev team!";
		}
	}

	private static void writeConfigs(BufferedWriter writer, int depth, IndexedAndLinkedMap<String, ComponentConfigDoc> configDescriptions) throws IOException {
		writeTag(writer, Tags.Begin.CONFIG_TABLE_CONTAINER, depth);

		writeTag(writer, Tags.Begin.get(Tags.Begin.DIV, Tags.Attribute.Key.CLASS, Tags.Attribute.Value.TABLE_RESPONSIVE), depth);

		writeTag(writer, Tags.Begin.CONFIG_TABLE, depth + 1);

		writeTag(writer, Tags.Begin.TABLE_HEAD, depth + 2);

		writeTag(writer, Tags.Begin.TABLE_ROW, depth + 3);
		writeTHeadData(writer, depth + 4, Tags.CSS.NAME_WIDTH_CLASS, "Name", "Default Title");
		writeTHeadData(writer, depth + 4, "Mandatory", "Default Title");
		writeTHeadData(writer, depth + 4, "Default Value", "Default Title");
		writeTHeadData(writer, depth + 4, Tags.CSS.DESCRIPTION_WIDTH_CLASS, "Description", "Default Title");
		writeTHeadData(writer, depth + 4, "Meta Similar To", "Default Title");
		writeTHeadData(writer, depth + 4, "Type", "Default Title");
		writeTag(writer, Tags.End.TABLE_ROW, depth + 3);

		writeTag(writer, Tags.End.TABLE_HEAD, depth + 2);

		writeTag(writer, Tags.Begin.TABLE_BODY, depth + 2);
		if (configDescriptions != null && configDescriptions.size() > 0) {
			for (ComponentConfigDoc configDescription : configDescriptions.values()) {
				writeConfigRow(writer, depth + 3, configDescription);
			}
		}
		writeTag(writer, Tags.End.TABLE_BODY, depth + 2);

		writeTag(writer, Tags.End.TABLE, depth + 1);

		writeTag(writer, Tags.End.DIV, depth);

		writeTag(writer, Tags.End.CONTAINER, depth);
	}

	private static void writeConfigRow(BufferedWriter writer, int depth, ComponentConfigDoc configDescription) throws IOException {
		writeTag(writer, Tags.Begin.TABLE_ROW, depth);

		writeTBodyData(writer, depth + 1, configDescription.getName(), "Name not specified by dev team!");
		writeTBodyData(writer, depth + 1, configDescription.getMandatory(), "Not specified by dev team!");
		writeTBodyData(writer, depth + 1, configDescription.getDefaultValue(), "");
		writeTBodyData(writer, depth + 1,
		        getConfigDescription(configDescription.getDescriptionDoc(), configDescription.canIgnoreFirstDescription(), configDescription.getHtmlTextStyle(),
		                depth + 1),
		        (configDescription.getHtmlTextStyle() != null && !configDescription.getHtmlTextStyle().equals(HTMLTextStyle.NONE)),
		        "Not specified by dev team!");
		writeTBodyData(writer, depth + 1, configDescription.getMeataSimilarTo(), "");
		writeTBodyData(writer, depth + 1, configDescription.getType(), "");

		writeTag(writer, Tags.End.TABLE_ROW, depth);
	}

	private static String getConfigDescription(ComponentConfigDescriptionDoc componentConfigDescriptionDoc, boolean ignoreFirst, HTMLTextStyle htmlTextStyle,
	        int depth) {
		if (htmlTextStyle == null) {
			htmlTextStyle = HTMLTextStyle.NONE;
		}

		String htmlText = "";

		for (int i = ignoreFirst ? 1 : 0; i < componentConfigDescriptionDoc.getChildren().size(); i++) {
			if (!htmlText.isEmpty()) {
				htmlText += htmlTextStyle.getLineEnd() + "\n" + StringUtil.nTimes(HelpConstants.General.TAB, depth);
			}

			String[] parts = componentConfigDescriptionDoc.getChildren().get(i).getChildren().get(0).getElement().split(":");

			if (parts.length == 2) {
				htmlText += htmlTextStyle.getLinePrefix() + Tags.Begin.ORDER_LIST_LABLE + parts[0] + Tags.End.ORDER_LIST_LABLE + Tags.Begin.SPAN + parts[1]
				        + Tags.End.SPAN + htmlTextStyle.getLineSufix();
			} else {
				htmlText += htmlTextStyle.getLinePrefix() + Tags.Begin.LABLE
				        + componentConfigDescriptionDoc.getChildren().get(i).getChildren().get(0).getElement() + Tags.End.LABLE + htmlTextStyle.getLineSufix();
			}

		}
		return (ignoreFirst
		        ? componentConfigDescriptionDoc.getChildren().get(0).getChildren().get(0).getElement() + "<br />\n"
		                + StringUtil.nTimes(HelpConstants.General.TAB, depth)
		        : "") + htmlTextStyle.getPrefix() + htmlText + htmlTextStyle.getSufix();
	}

	private static void writeSemiTHeadRow(BufferedWriter writer, int depth, String title, String value, String defaultValue) throws IOException {
		writeTag(writer, Tags.Begin.TABLE_ROW, depth);
		writeTHeadData(writer, depth + 1, title, "Default Title");
		writeTBodyData(writer, depth + 1, value, defaultValue);
		writeTag(writer, Tags.End.TABLE_ROW, depth);
	}

	private static void writeTBodyData(BufferedWriter writer, int depth, String lable, String defaultValue) throws IOException {
		writeTBodyData(writer, depth, lable, false, defaultValue);
	}

	private static void writeTBodyData(BufferedWriter writer, int depth, String lable, boolean avoidLabel, String defaultValue) throws IOException {
		writeTag(writer, Tags.Begin.TABLE_DATA, depth);
		if (avoidLabel) {
			writeTag(writer, lable == null ? defaultValue : lable, depth + 1);
		} else {
			writeLable(writer, depth + 1, lable, defaultValue);
		}
		writeTag(writer, Tags.End.TABLE_DATA, depth);
	}

	private static void writeTHeadData(BufferedWriter writer, int depth, String lable, String defaultValue) throws IOException {
		writeTag(writer, Tags.Begin.TABLE_HEADER_DATA, depth);
		writeLable(writer, depth + 1, lable, defaultValue);
		writeTag(writer, Tags.End.TABLE_HEADER_DATA, depth);
	}

	private static void writeTHeadData(BufferedWriter writer, int depth, String cssClass, String lable, String defaultValue) throws IOException {
		writeTag(writer, Tags.Begin.UNCLOSED_TABLE_HEADER_DATA + cssClass + Tags.TAG_CLOSE, depth);
		writeLable(writer, depth + 1, lable, defaultValue);
		writeTag(writer, Tags.End.TABLE_HEADER_DATA, depth);
	}

	private static void writeLable(BufferedWriter writer, int depth, String lable, String defaultValue) throws IOException {
		writeTag(writer, Tags.Begin.LABLE, depth);
		writeTag(writer, lable == null ? defaultValue : lable, depth + 1);
		writeTag(writer, Tags.End.LABLE, depth);
	}

	private static void writeHead(BufferedWriter writer) throws IOException {
		writeTag(writer, Tags.Begin.HEAD, 1);
		writeTag(writer, getTitle(), 2);
		writeTag(writer, getMetaCharset(), 2);
		writeTag(writer, getMetaDeviceWidth(), 2);
		writeTag(writer, getBootStrapCSS(), 2);
		writeTag(writer, getCSS(), 2);
		writeTag(writer, getJQueryJS(), 2);
		writeTag(writer, getPopperJS(), 2);
		writeTag(writer, getBootStrapJS(), 2);
		writeTag(writer, Tags.End.HEAD, 1);
	}

	private static String getMetaCharset() {
		return "<meta charset=\"utf-8\">";
	}

	private static String getMetaDeviceWidth() {
		return "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">";
	}

	private static String getJQueryJS() {
		return "<script src=\"https://ajax.googleapis.com/ajax/libs/jquery/3.4.1/jquery.min.js\"></script>";
	}

	private static String getPopperJS() {
		return "<script src=\"https://cdnjs.cloudflare.com/ajax/libs/popper.js/1.16.0/umd/popper.min.js\"></script>";
	}

	private static String getBootStrapJS() {
		return "<script src=\"https://maxcdn.bootstrapcdn.com/bootstrap/4.4.1/js/bootstrap.min.js\"></script>";
	}

	private static String getBootStrapCSS() {
		return "<link rel=\"stylesheet\" href=\"https://maxcdn.bootstrapcdn.com/bootstrap/4.4.1/css/bootstrap.min.css\">";
	}

	private static String getCSS() {
		return "<link rel=\"stylesheet\" type=\"text/css\" href=\"anvizent.elt.help.css\">";
	}

	private static String getTitle() {
		return Tags.Begin.TITLE + HelpConstants.General.TITLE + Tags.End.TITLE;
	}

	private static BufferedWriter getHTMLPage(File parentDirectory) throws IOException {
		File htmlPage = new File(parentDirectory, HelpConstants.General.HTML_PAGE_PATH);
		if (htmlPage.exists()) {
			htmlPage.delete();
		}
		htmlPage.createNewFile();

		return new BufferedWriter(new FileWriter(htmlPage));
	}
}
