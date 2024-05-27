package org.hadoop.hdfs.serialize;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;

import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

// Serialize dữ liệu trước khi được chuyển đi hoặc lưu trữ
// Serialize thành định dạng byte trước khi chuyển đi hoặc lưu trữ dữ liệu trên hadoop
// Cách này tương đối cũ so với hiện nay sử dụng Writable

public class DemoJavaSerialize {
	@Test
	public void serialize() throws Exception {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(new Student());
		oos.close();
		baos.close();
	}
	@Test
	public void writable() throws Exception{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		IntWritable i = new IntWritable(255);
		i.write(oos);
		oos.close();
		baos.close();
		byte[] buf = baos.toByteArray();
		System.out.println(buf.length);
	}
}
