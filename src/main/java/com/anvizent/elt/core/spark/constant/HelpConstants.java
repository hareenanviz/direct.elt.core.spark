package com.anvizent.elt.core.spark.constant;

/**
 * @author Hareen Bejjanki
 * @author Apurva Deshmukh
 *
 */
public class HelpConstants {
	public static class General {
		public static final String HELP = "--help";
		public static final String HELP_SHORT = "-h";
		public static final String HTML = "html";
		public static final String HTML_PAGE_PATH = "anvizent.elt.help.html";
		public static final String TITLE = "Anvizent ELT Core help doc";
		public static final String SOURCES_TITLE = "SOURCES";
		public static final String OPERATIONS_TITLE = "OPERATIONS";
		public static final String FILTERS_TITLE = "FILTER";
		public static final String SINKS_TITLE = "STORES";
		public static final String MAPPING_OPERATIONS_TITLE = "MAPPING OPERATIONS";
		public static final String TAB = "    ";
		public static final String NEW_LINE = "\n";
	}

	public static class Type {
		public static final String STRING = "String";
		public static final String LONG = "Long";
		public static final String INTEGER = "Integer";
		public static final String LIST_OF_INTEGERS = "List of Integers in CSV Format";
		public static final String LIST_OF_STRINGS = "List of Strings in CSV Format";
		public static final String LIST_OF_DATE_FORMATS = "List of Date Formats as Strings in CSV Format";
		public static final String LIST_OF_TYPES = "List of Types as Strings in CSV Format";
		public static final String BOOLEAN = "Boolean (true/false)";
	}

	public static class Tags {
		public static final String TAG_CLOSE = ">";

		public static class CSS {
			public static final String NAME_WIDTH_CLASS = "class=\"name_width\"";
			public static final String DESCRIPTION_CLASS = "class=\"description\"";
			public static final String DESCRIPTION_WIDTH_CLASS = "class=\"description_width\"";
			public static final String CONFIGS_CLASS = "class=\"config\"";
		}

		public static class Begin {
			public static final String HTML = "<html>";
			public static final String TITLE = "<title>";
			public static final String HEAD = "<head>";
			public static final String BODY = "<body>";
			public static final String MAIN_TABLE = "<table class=\"main_table\" cellspacing=\"10\">";
			public static final String COMPONENT_TABLE = "<table  class=\"table table-striped\">";
			public static final String CONFIG_TABLE_CONTAINER = "<div class=\"table table-striped\">";
			public static final String CONFIG_TABLE = "<table  class=\"table table-striped\" cellspacing=\"1\">";

			public static final String TABLE_HEAD = "<thead class=\"thead-dark\">";
			public static final String TABLE_BODY = "<tbody>";
			public static final String TABLE_ROW = "<tr>";
			public static final String TABLE_HEADER_DATA = "<th>";
			public static final String UNCLOSED_TABLE_HEADER_DATA = "<th ";
			public static final String TABLE_HEADER_DATA_WITH_CLASS_BG = "<th class=\"bg\">";
			public static final String TABLE_DATA = "<td>";
			public static final String LABLE = "<label>";
			public static final String ORDER_LIST_LABLE = "<label class=\"config_orderList\">";
			public static final String SPAN = "<span> ";
			public static final String DIV = "<div>";
			public static final String UL = "<ul>";
			public static final String LI = "<li>";
			public static final String A = "<a>";
			public static final String BREAK = "<br />";

			public static final String get(String tag, String... atributes) {
				String tagWithAttrs = tag.replace('>', ' ');

				String next = "=\"";
				for (String atribute : atributes) {
					tagWithAttrs += atribute + next;
					if (next.equals("=\"")) {
						next = "\" ";
					} else {
						next = "=\"";
					}
				}

				return tagWithAttrs + ">";
			}
		}

		public static class End {
			public static final String HTML = "</html>";
			public static final String TITLE = "</title>";
			public static final String HEAD = "</head>";
			public static final String BODY = "</body>";
			public static final String TABLE = "</table>";
			public static final String CONTAINER = "</div>";
			public static final String TABLE_HEAD = "</thead>";
			public static final String TABLE_BODY = "</tbody>";
			public static final String TABLE_ROW = "</tr>";
			public static final String TABLE_HEADER_DATA = "</th>";
			public static final String TABLE_DATA = "</td>";
			public static final String LABLE = "</label>";
			public static final String ORDER_LIST_LABLE = ":</label>";
			public static final String SPAN = "</span>";
			public static final String DIV = "</div>";
			public static final String UL = "</ul>";
			public static final String LI = "</li>";
			public static final String A = "</a>";
		}

		public static class Attribute {
			public static class Key {
				public static final String CLASS = "class";
				public static final String ROLE = "role";
				public static final String DATA_TOGGLE = "data-toggle";
				public static final String ID = "id";
				public static final String HREF = "href";
			}

			public static class Value {
				public static final String CONTAINER = "container";
				public static final String TABS = "nav nav-tabs";
				public static final String TABS_LIST = "tablist";
				public static final String TAB_ITEM = "nav-item";
				public static final String ACTIVE_TAB_LINK = "nav-link active";
				public static final String NORMAL_TAB_LINK = "nav-link";
				public static final String TAB = "tab";
				public static final String TAB_CONTENT = "tab-content";
				public static final String ACTIVE_TAB_CONTAINER = "container tab-pane active";
				public static final String NORMAL_TAB_CONTAINER = "container tab-pane fade";
				public static final String TABLE_RESPONSIVE = "table-responsive";
			}
		}
	}
}
