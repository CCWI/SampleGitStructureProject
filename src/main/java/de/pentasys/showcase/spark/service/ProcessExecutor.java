package de.pentasys.showcase.spark.service;

import java.io.Serializable;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class ProcessExecutor implements Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(ProcessExecutor.class);

	public ProcessExecutor() {
		super();
	}

	public void processTraceLogLines(@NotNull String key, @NotNull String value) {

		LOG.info("Received Message for processing in Spark - Key: {} - Value: {}", key, value);

		// further processing
	}

}