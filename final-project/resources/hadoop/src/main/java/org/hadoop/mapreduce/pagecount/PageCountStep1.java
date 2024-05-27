package org.hadoop.mapreduce.pagecount;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * Thống kê lưu lượng truy cập trang basic
 */
public class PageCountStep1{
	
	public static class PageSortStep1Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] split = line.split(" ");
			context.write(new Text(split[1]), new IntWritable(1));
		}
	}
	
	public static class PageSortStep1Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable v : values) {
				count += v.get();
			}
			context.write(key, new IntWritable(count));
		}
	}
	
	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", "C:\\test\\winutils-master\\hadoop-3.1.1");
		
		Configuration configuration = new Configuration();
//		configuration.addResource("mapred-site.xml");
		
		
		Job job = Job.getInstance(configuration);
		
		job.setJarByClass(PageCountStep1.class);
		
		job.setMapperClass(PageSortStep1Mapper.class);
		job.setReducerClass(PageSortStep1Reducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
//		FileSystem fs = FileSystem.get(configuration);

//		Path outputDirPath = new Path("E:\\MAIN\\DuLieuLon(BigData)\\Demo\\testfile\\output");
//
//		if (fs.exists(outputDirPath)) {
//		    fs.delete(outputDirPath, true);
//		}
		
		FileInputFormat.setInputPaths(job, new Path("E:\\MAIN\\DuLieuLon(BigData)\\Demo\\testfile\\input"));
		FileOutputFormat.setOutputPath(job, new Path("E:\\MAIN\\DuLieuLon(BigData)\\Demo\\testfile\\count-output"));
		
		job.waitForCompletion(true);
	}
}
