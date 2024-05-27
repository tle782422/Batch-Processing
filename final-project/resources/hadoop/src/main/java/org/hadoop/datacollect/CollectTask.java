package org.hadoop.datacollect;


import java.io.File;
import java.io.FilenameFilter;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.TimerTask;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.hadoop.common.constant.Constants;


public class CollectTask extends TimerTask {

	@Override
	public void run() {
		
		try {
		// Dùng 1 trong 2 cách để tải cấu hình của file collect.
		Properties props = PropertyHolderLazy.getProps();
			
		Logger logger = Logger.getLogger("file");
		
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH");
		String date = simpleDateFormat.format(new Date());
		
		File srcDir = new File(props.getProperty(Constants.LOG_SOURCE_DIR));
		File[] listFiles = srcDir.listFiles(new FilenameFilter() {
			
			@Override
			public boolean accept(File dir, String name) {
				if(name.startsWith(props.getProperty(Constants.LOG_LEGAL_FREFIX))) {
					return true;
				}
				return false;
			}
		});	
		
		logger.info(Arrays.toString(listFiles));	
			
		File toUploadDir = new File(props.getProperty(Constants.LOG_TOUPLOAD_DIR));
		for(File file: listFiles) {
			FileUtils.moveFileToDirectory(file, toUploadDir,true);
			
		}
		
		logger.info("---------------------------------------");
		logger.info(toUploadDir.getAbsolutePath());
		logger.info("---------------------------------------");
		

		FileSystem fileSystem = FileSystem.get(new URI(props.getProperty(Constants.HDFS_URI)), new Configuration(), "toanle");
		File[] toUploadFiles = toUploadDir.listFiles();
		
		Path hdfsDestPath = new Path(props.getProperty(Constants.HDFS_DEST_BASE_DIR) + date);
		if(!fileSystem.exists(hdfsDestPath)) {
			fileSystem.mkdirs(hdfsDestPath);
		}
		
		File backupDir = new File(props.getProperty(Constants.LOG_BACKUP_BASE_DIR)+date+"/");
		if(!backupDir.exists()) {
			backupDir.mkdirs();
		}
		
		for(File file : toUploadFiles) {
			Path destPath = new Path(
					 hdfsDestPath+props.getProperty(Constants.HDFS_FILE_FREFIX)
					+UUID.randomUUID()
					+hdfsDestPath+props.getProperty(Constants.HDFS_FILE_SUFFIX));
			
			fileSystem.copyFromLocalFile(new Path(file.getAbsolutePath()), destPath);
				
			logger.info(file.getAbsolutePath()+ "--->"+destPath);
					
			FileUtils.moveFileToDirectory(file, backupDir,true);
			//file.renameTo(backupDir);
			
			logger.info(file.getAbsolutePath()+ "--->"+backupDir);
			
		}
		fileSystem.close();
		} catch (Exception ex) {
			System.out.println(ex.toString());
			//ex.printStackTrace();
		}
	}
}
