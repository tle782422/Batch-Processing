package org.hadoop.mapreduce.topn.grouping;

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
import org.hadoop.mapreduce.topn.OrderBean;

public class OrderTopn {
	
	
	
	public static class OrderTopnMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable>{

		OrderBean orderBean = new OrderBean();
		NullWritable v = NullWritable.get();
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, OrderBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String[] fields = value.toString().split(",");
			orderBean.set(fields[0], fields[1], fields[2], Float.parseFloat(fields[3]), Integer.parseInt(fields[4]));
			
			context.write(orderBean,v);
		}
	}
	
	public static class OrderTopnReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {
		@Override
		protected void reduce(OrderBean key, Iterable<NullWritable> values,
				Reducer<OrderBean, NullWritable, OrderBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			int i=0;
			for (NullWritable v : values) {
				context.write(key, v);
				if(++i==3) return;
			}}
	   	    
	}
	
	// Nhóm và sắp xếp order bằng GroupingComparator
	
	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", "C:\\test\\winutils-master\\hadoop-3.1.1");
		
		Configuration configuration = new Configuration();
		configuration.setInt("order.top.n", 2);
		
		Job job = Job.getInstance(configuration);
		
		job.setJarByClass(OrderTopn.class);
		
		job.setMapperClass(OrderTopnMapper.class);
		job.setReducerClass(OrderTopnReducer.class);
		
		job.setPartitionerClass(OrderIdPartitioner.class);
		job.setGroupingComparatorClass(OrderIdGroupingComparator.class);
		
		job.setNumReduceTasks(2);
		
		job.setMapOutputKeyClass(OrderBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(OrderBean.class);
		job.setOutputValueClass(NullWritable.class);
		

		
		FileInputFormat.setInputPaths(job, new Path("E:\\MAIN\\DuLieuLon(BigData)\\Demo\\testfile\\orders"));
		FileOutputFormat.setOutputPath(job, new Path("E:\\MAIN\\DuLieuLon(BigData)\\Demo\\testfile\\order2-output"));
		
		job.waitForCompletion(true);
	}
	
}
