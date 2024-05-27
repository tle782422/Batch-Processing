package org.hadoop.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * https://hadoop.apache.org/docs/r2.8.2/api/org/apache/hadoop/mapreduce/Mapper.html
 */
public class WordCountMapper extends Mapper<LongWritable,Text , Text, IntWritable> {
	
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
			String line = value.toString();
			String[] words = line.split(" ");
			for(String word:words) {
				context.write(new Text(word), new IntWritable(1));
				
			}

	}
}
