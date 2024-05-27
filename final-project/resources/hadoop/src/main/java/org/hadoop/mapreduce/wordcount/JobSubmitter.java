package org.hadoop.mapreduce.wordcount;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



//https://hadoop.apache.org/docs/r2.7.1/hadoop-mapreduce-client/hadoop-mapreduce-client-core/mapred-default.xml
//https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/core-default.html
//https://hadoop.apache.org/docs/r3.0.3/hadoop-yarn/hadoop-yarn-common/yarn-default.xml

//https://community.cloudera.com/t5/Support-Questions/Permission-denied-user-admin-access-WRITE-inode-quot-user/m-p/243825

public class JobSubmitter {
	public static void main(String[] args) throws Exception {
		
//		System.setProperty("HADOOP_USER_NAME", "hadoop");
//		PropertyConfigurator.configure("E:/MAIN/DuLieuLon(BigData)/Demo/src/main/resources/log4j.properties");
		System.setProperty("hadoop.home.dir", "C:\\test\\winutils-master\\hadoop-3.1.1");
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		conf.set("dfs.replication", "2");
		conf.set("dfs.blocksize", "64m");	
//		conf.set("mapreduce.framework.name", "yarn");
//		conf.set("yarn.resourcemanager.hostname", "master");
//		conf.set("mapreduce.app-submission.cross-platform", "true");
//		conf.set("mapreduce.job.log4j-properties-file", "src/main/resources/log4j.properties");
		
//         Set the required properties if not already set
//        if (conf.get("yarn.app.mapreduce.am.env") == null) {
//            conf.set("yarn.app.mapreduce.am.env", "HADOOP_MAPRED_HOME=${HADOOP_HOME}");
//        }
//        if (conf.get("mapreduce.map.env") == null) {
//            conf.set("mapreduce.map.env", "HADOOP_MAPRED_HOME=${HADOOP_HOME}");
//        }
//        if (conf.get("mapreduce.reduce.env") == null) {
//            conf.set("mapreduce.reduce.env", "HADOOP_MAPRED_HOME=${HADOOP_HOME}");
//        }
//        // Merge with existing configuration
//        conf.addResource(new Path(System.getenv("HADOOP_HOME") + "/etc/hadoop/mapred-site.xml"));
		
		
		
		Job job = Job.getInstance(conf);
		
		job.setJar("C:/wc.jar");
		job.setJarByClass(JobSubmitter.class);
		
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		Path output = new Path("/wordcount/output");
		FileSystem fs = FileSystem.get(new URI("hdfs://master:9000/"), conf, "hadoop");
		if(fs.exists(output)) {
			fs.delete(output,true);
		}
		
		FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
		FileOutputFormat.setOutputPath(job, new Path("/wordcount/output"));
	
		job.setNumReduceTasks(2);
		
		boolean res = job.waitForCompletion(true);
		
		System.exit(res?0:1);
//		job.submit();
	}
}
