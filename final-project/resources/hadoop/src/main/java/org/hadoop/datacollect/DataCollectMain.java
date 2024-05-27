package org.hadoop.datacollect;

import java.util.Timer;
// LOGGER CONFIG BASIC
public class DataCollectMain {
	public static void main(String[] args) {
		// Log ở HDFS
		Timer timer = new Timer();
		timer.schedule(new CollectTask(), 0, 60*60*1000L);
//		timer.schedule(new BackupCleanTask(), 0, 60*60*1000L);
	}
}
