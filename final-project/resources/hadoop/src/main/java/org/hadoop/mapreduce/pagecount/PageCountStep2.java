package org.hadoop.mapreduce.pagecount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Page Sort bước 2 sắp xếp từ thư mục PageCountStep1
 */
public class PageCountStep2 {

	public static class PageSortStep2Mapper extends Mapper<LongWritable, Text, PageCount, NullWritable>{
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, PageCount, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			
			PageCount pageCount = new PageCount();
			pageCount.set(split[0], Integer.parseInt(split[1]));
			
			context.write(pageCount, NullWritable.get());
		}
	}
	
	public static class PageSortStep2Reducer extends Reducer<PageCount, NullWritable, PageCount, NullWritable>{
		@Override
		protected void reduce(PageCount key, Iterable<NullWritable> values,
				Reducer<PageCount, NullWritable, PageCount, NullWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(key, NullWritable.get());}
	}
	
	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", "C:\\test\\winutils-master\\hadoop-3.1.1");
		
		Configuration configuration = new Configuration();
//		configuration.addResource("mapred-site.xml");
		
		
		Job job = Job.getInstance(configuration);
		
		job.setJarByClass(PageCountStep2.class);
		
		job.setMapperClass(PageSortStep2Mapper.class);
		job.setReducerClass(PageSortStep2Reducer.class);
		
		job.setMapOutputKeyClass(PageCount.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(PageCount.class);
		job.setOutputValueClass(NullWritable.class);
		
//		FileSystem fs = FileSystem.get(configuration);

//		Path outputDirPath = new Path("E:\\MAIN\\DuLieuLon(BigData)\\Demo\\testfile\\output");
//
//		if (fs.exists(outputDirPath)) {
//		    fs.delete(outputDirPath, true);
//		}
		
		FileInputFormat.setInputPaths(job, new Path("E:\\MAIN\\DuLieuLon(BigData)\\Demo\\testfile\\count-output"));
		FileOutputFormat.setOutputPath(job, new Path("E:\\MAIN\\DuLieuLon(BigData)\\Demo\\testfile\\sort-output"));
		
		job.setNumReduceTasks(1);
		
		job.waitForCompletion(true);
	}
	
}
