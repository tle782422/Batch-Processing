package org.hadoop.datacollect;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimerTask;

import org.apache.commons.io.FileUtils;

public class BackupCleanTask extends TimerTask {

	@Override
	public void run() {
		
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH");
		long now = new Date().getTime();
		try {
		File backupBaseDir = new File("logs/backup/");
		File[] dayBackDir = backupBaseDir.listFiles();

		for(File dir : dayBackDir) {
			long time = simpleDateFormat.parse(dir.getName()).getTime();
			if(now-time>24*60*60*1000L) {
				FileUtils.deleteDirectory(dir);
			}
		}
		
		} catch(Exception ex) {
			System.out.println(ex.toString());
		}
		
	}

}
