package org.hadoop.mapreduce.topn.grouping;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.hadoop.mapreduce.topn.OrderBean;

public class OrderIdPartitioner extends Partitioner<OrderBean, NullWritable> {

	@Override
	public int getPartition(OrderBean key, NullWritable value, int numPartitions) {
		// TODO Auto-generated method stub
		return (key.getOrderId().hashCode() & Integer.MAX_VALUE)%numPartitions;
	}
	
	

}
