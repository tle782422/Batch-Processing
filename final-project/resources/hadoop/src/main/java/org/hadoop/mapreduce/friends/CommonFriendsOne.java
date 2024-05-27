package org.hadoop.mapreduce.friends;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CommonFriendsOne {
	//A:B,C,D,E,F,O <=> A->B , A->C , A->D , ...
	public static class CommonFriendsOneMapper extends Mapper<LongWritable, Text, Text, Text>{
		Text k = new Text();
		Text v = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] userAndFriends = value.toString().split(":");
			
			String user = userAndFriends[0];
			String[] friends = userAndFriends[1].split(",");
			v.set(user);
			for (String f : friends) {
				k.set(f);
				context.write(k, v);
			}
		}
	}
	
	public static class CommonFriendsOneReducer extends Reducer<Text, Text, Text, Text>{
		// B --> A E F J ...
		// C --> B F E J ...
		@Override
		protected void reduce(Text friend, Iterable<Text> users, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			ArrayList<String> userList = new ArrayList<>();
			for (Text user : users) {
				userList.add(user.toString());
			}
			Collections.sort(userList);
			for (int i = 0; i < userList.size(); i++) {
				for (int j = i+1; j < userList.size(); j++) {
					context.write(new Text(userList.get(i)+"-"+userList.get(j)), friend);
				}
			}
			
		}

	}
	
	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", "C:\\test\\winutils-master\\hadoop-3.1.1");
		
		Configuration configuration = new Configuration();
		configuration.setInt("order.top.n", 2);
		
		Job job = Job.getInstance(configuration);
		
		job.setJarByClass(CommonFriendsOne.class);
		
		job.setMapperClass(CommonFriendsOneMapper.class);
		job.setReducerClass(CommonFriendsOneReducer.class);
		
//		job.setPartitionerClass(OrderIdPartitioner.class);
//		job.setGroupingComparatorClass(OrderIdGroupingComparator.class);
		
//		job.setNumReduceTasks(2);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		

		
		FileInputFormat.setInputPaths(job, new Path("E:\\MAIN\\DuLieuLon(BigData)\\Demo\\testfile\\friends"));
		FileOutputFormat.setOutputPath(job, new Path("E:\\MAIN\\DuLieuLon(BigData)\\Demo\\testfile\\friends\\output"));
		
		job.waitForCompletion(true);
	}
}





