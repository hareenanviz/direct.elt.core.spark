package com.anvizent.elt.core.spark.source.config.bean;

import com.anvizent.elt.core.lib.config.bean.ConfigBean;
import com.anvizent.elt.core.listener.common.sink.ErrorHandlerSink;
import com.anvizent.elt.core.spark.sink.config.bean.SinkConfigBean;

/**
 * @author Hareen Bejjanki
 *
 */
public class ConsoleSinkConfigBean extends ConfigBean implements SinkConfigBean, ErrorHandlerSink {

	private static final long serialVersionUID = 1L;

	private boolean writeAll;

	public boolean isWriteAll() {
		return writeAll;
	}

	public void setWriteAll(boolean writeAll) {
		this.writeAll = writeAll;
	}

}
