package org.hadoop.mapreduce.skewkey;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.eclipse.jetty.websocket.client.masks.RandomMasker;

import java.io.IOException;
import java.util.Random;

public class SkewWordCount {

    public static class SkewMapper extends Mapper<LongWritable, Text,Text, IntWritable>{
        Random random = new Random();
        Text k = new Text();
        IntWritable v = new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            int numReduceTasks = context.getNumReduceTasks();
            String[] words = value.toString().split(" ");
            for (String w : words){
                k.set(w+"-"+random.nextInt(numReduceTasks));
                context.write(k,v);
            }
        }
    }

    public static class SkewReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        IntWritable v = new IntWritable(1);
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int count = 0;
            for(IntWritable value : values){
                count+=value.get();
            }
            v.set(count);
            context.write(key,v);
        }
    }

}
