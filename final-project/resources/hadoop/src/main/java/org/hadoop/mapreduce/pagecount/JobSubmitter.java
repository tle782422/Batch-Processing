package org.hadoop.mapreduce.pagecount;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.hadoop.datacollect.PropertyHolderHungery;

public class JobSubmitter {
	
	/**
	 *  Thiết kế mô hình lập trình Mapreduce 
	 *  với số lượt truy cập trang cao nhất
	 *  sử dụng sơ đồ cây TreeMap<key,value>
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", "C:\\test\\winutils-master\\hadoop-3.1.1");
		
		Configuration configuration = new Configuration();
//		configuration.addResource("mapred-site.xml");
		
		Properties props = new Properties();
		props.load(JobSubmitter.class
				.getClassLoader()
				.getResourceAsStream("job.properties"));
		configuration.setInt("top.n", Integer.parseInt(props.getProperty("top.n")));
		
		Job job = Job.getInstance(configuration);
		
		job.setJarByClass(JobSubmitter.class);
		
		job.setMapperClass(PageTopMapper.class);
		job.setReducerClass(PageTopReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// Tạo một đối tượng FileSystem từ Configuration
		FileSystem fs = FileSystem.get(configuration);

		// Tạo đường dẫn đến thư mục đầu ra
		Path outputDirPath = new Path("E:\\MAIN\\DuLieuLon(BigData)\\Demo\\testfile\\output");

		// Kiểm tra xem thư mục đầu ra đã tồn tại hay không
		if (fs.exists(outputDirPath)) {
		    // Nếu tồn tại, xóa thư mục đầu ra và tạo một thư mục mới
		    fs.delete(outputDirPath, true);
		}
		
		FileInputFormat.setInputPaths(job, new Path("E:\\MAIN\\DuLieuLon(BigData)\\Demo\\testfile\\input"));
		FileOutputFormat.setOutputPath(job, new Path("E:\\MAIN\\DuLieuLon(BigData)\\Demo\\testfile\\output"));
		
		job.waitForCompletion(true);
	}
}
