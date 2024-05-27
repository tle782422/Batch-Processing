package org.hadoop;

import org.apache.log4j.Logger;

public class LoggerWriter {
	/**
	 * Test Logger
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		while(true) {
			Logger logger = Logger.getLogger("file");
			logger.info("1111111111111111111111111110----" + System.currentTimeMillis());
			Thread.sleep(50);
		}
	}
}
