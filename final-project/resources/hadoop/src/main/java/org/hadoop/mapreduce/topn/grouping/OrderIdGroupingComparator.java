package org.hadoop.mapreduce.topn.grouping;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.hadoop.mapreduce.topn.OrderBean;

public class OrderIdGroupingComparator extends WritableComparator {

	//Kế thừa lên lớp cha OrderBean bằng super
	//được sử dụng để gọi constructor của lớp cha WritableComparator
	public OrderIdGroupingComparator() {
		super(OrderBean.class,true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		OrderBean o1 = (OrderBean) a;
		OrderBean o2 = (OrderBean) b;
		return o1.getOrderId().compareTo(o2.getOrderId());
	}
}
