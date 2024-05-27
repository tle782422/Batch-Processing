package org.hadoop.mapreduce.wordcount;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;


//CACH 2
public class App {
	public static void main(String[] args) throws Exception {	
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		System.setProperty("hadoop.home.dir", "C:\\test\\winutils-master\\hadoop-3.1.1");
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		conf.set("dfs.replication", "2");
		conf.set("dfs.blocksize", "64m");	
//		conf.set("mapreduce.framework.name", "yarn");
//		conf.set("yarn.resourcemanager.hostname", "master");
//		conf.set("mapreduce.app-submission.cross-platform", "true");
////		conf.set("mapreduce.job.log4j-properties-file", "src/main/resources/log4j.properties");
//		
        // Set the required properties if not already set
        if (conf.get("yarn.app.mapreduce.am.env") == null) {
            conf.set("yarn.app.mapreduce.am.env", "HADOOP_MAPRED_HOME=/home/hadoop/hadoop");
        }
        if (conf.get("mapreduce.map.env") == null) {
            conf.set("mapreduce.map.env", "HADOOP_MAPRED_HOME=/home/hadoop/hadoop");
        }
        if (conf.get("mapreduce.reduce.env") == null) {
            conf.set("mapreduce.reduce.env", "HADOOP_MAPRED_HOME=/home/hadoop/hadoop");
        }

//        conf.addResource(new Path(System.getenv("HADOOP_HOME") + "/etc/hadoop/mapred-site.xml"));
//		  conf.set("yarn.application.classpath",
//	              "{{HADOOP_CONF_DIR}},{{HADOOP_COMMON_HOME}}/share/hadoop/common/*,{{HADOOP_COMMON_HOME}}/share/hadoop/common/lib/*,"
//	                  + " {{HADOOP_HDFS_HOME}}/share/hadoop/hdfs/*,{{HADOOP_HDFS_HOME}}/share/hadoop/hdfs/lib/*,"
//	                  + "{{HADOOP_MAPRED_HOME}}/share/hadoop/mapreduce/*,{{HADOOP_MAPRED_HOME}}/share/hadoop/mapreduce/lib/*,"
//	                  + "{{HADOOP_YARN_HOME}}/share/hadoop/yarn/*,{{HADOOP_YARN_HOME}}/share/hadoop/yarn/lib/*");
        
		Path output = new Path("/wordcount/output");
		FileSystem fs = FileSystem.get(new URI("hdfs://master:9000/"), conf, "hadoop");
		if(fs.exists(output)) {
			fs.delete(output,true);
		}
        
		int res = ToolRunner.run(conf, new WordCount(), args);
		System.exit(res);
	}
}
