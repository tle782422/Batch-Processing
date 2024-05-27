package org.hadoop.hdfs.sample;


//Giao diện Mapper tự tạo
public interface Mapper {

	public void map(String line,Context context);
}
