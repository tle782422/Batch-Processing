package org.hadoop.mapreduce.topn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

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

public class OrderTopn {

	
	public static class OrderTopnMapper extends Mapper<LongWritable, Text, Text, OrderBean>{
		OrderBean orderBean = new OrderBean();
		Text orderKey = new Text();

		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, OrderBean>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split(",");
			
			orderBean.set(fields[0], fields[1], fields[2], Float.parseFloat(fields[3]), Integer.parseInt(fields[4]));
			orderKey.set(fields[0]);
			context.write(orderKey, orderBean);
			
		}
	}
	
	public static class OrderTopnReducer extends Reducer<Text, OrderBean, OrderBean, NullWritable> {

	    @Override
	    protected void reduce(Text key, Iterable<OrderBean> values,
	                          Reducer<Text, OrderBean, OrderBean, NullWritable>.Context context)
	            throws IOException, InterruptedException {
	    	
	        int topn = context.getConfiguration().getInt("order.top.n", 3);

	        ArrayList<OrderBean> beanList = new ArrayList<>();

	        for (OrderBean orderBean : values) {
	            OrderBean newBean = new OrderBean();
	            newBean.set(orderBean.getOrderId(), orderBean.getUserId(), orderBean.getProductName(), orderBean.getPrice(), orderBean.getQuantity());
	            beanList.add(newBean);
	        }


	        // Sắp xếp danh sách beanList
	        Collections.sort(beanList);

	        // Truy xuất và ghi ra context
	        for (int i = 0; i < topn; i++) {
	            context.write(beanList.get(i), NullWritable.get());
	        }
	    }
	}
	
	
	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", "C:\\test\\winutils-master\\hadoop-3.1.1");
		
		Configuration configuration = new Configuration();
		configuration.setInt("order.top.n", 2);
		
		Job job = Job.getInstance(configuration);
		
		job.setJarByClass(OrderTopn.class);
		
		job.setMapperClass(OrderTopnMapper.class);
		job.setReducerClass(OrderTopnReducer.class);
		
		job.setNumReduceTasks(2);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(OrderBean.class);
		
		job.setOutputKeyClass(OrderBean.class);
		job.setOutputValueClass(NullWritable.class);
		

		
		FileInputFormat.setInputPaths(job, new Path("E:\\MAIN\\DuLieuLon(BigData)\\Demo\\testfile\\orders"));
		FileOutputFormat.setOutputPath(job, new Path("E:\\MAIN\\DuLieuLon(BigData)\\Demo\\testfile\\order-output"));
		
		job.waitForCompletion(true);
	}
	
}
