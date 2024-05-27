package org.hadoop.hdfs;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;
//https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/hdfs-default.xml

public class HdfsClientDemo {
	public static void main(String[] args) throws Exception {
//		System.setProperty("hadoop.home.dir", "C:\\test\\winutils-master\\hadoop-3.1.1");
		// Mặc định cấu hình hdfs-site, core-site, yarn-site,...
		Configuration conf = new Configuration();
		conf.set("dfs.replication", "1");
		conf.set("dfs.blocksize", "64m");	
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://master:9000/"), conf, "hadoop");
		
		// Test đẩy file lên hdfs		
		fileSystem.copyFromLocalFile(new Path("C:/cmdlinux.txt"), new Path("/demo1/"));
		
		fileSystem.close();
	}
	
	
	///
	/**
	 * Test copy file từ ổ cứng vào hdfs và những thao tác cơ bản khác
	 * Test đọc file từ hdfs
	 * Test ghi file vào hdfs
	 */
	///
	FileSystem fs = null;
	/**
	 * Khởi tạo cấu hình cho hdfs ở mức basic
	 * @throws Exception
	 */
	@Before
	public void init() throws Exception{
		Configuration conf = new Configuration();
		conf.set("dfs.replication", "2");
		conf.set("dfs.blocksize", "64m");		
		fs = FileSystem.get(new URI("hdfs://master:9000/"), conf, "toanle");
	}
	/**
	 * test tải file từ local lên máy ảo hadoop
	 * @throws Exception
	 */
	@Test
	public void testGet() throws Exception {
		fs.copyFromLocalFile(new Path("D:/intern-fsoft/linux.iso"), new Path("/demo/"));
		fs.close();
	}
	/**
	 * test đổi tên file 
	 * @throws Exception
	 */
	@Test
	public void testRename() throws Exception{
//		fs.rename(new Path(""), new Path(""));
//		fs.close();
	}
	/**
	 * test tạo thư mục trên máy ảo
	 * @throws Exception
	 */
	@Test
	public void testMkdir() throws Exception{
		fs.mkdirs(new Path("orders/nam/thang/ngay/"));
		fs.close();
	}
	/**
	 * test xóa thư mục trên máy ảo
	 * @throws Exception
	 */
	@Test
	public void testRemove() throws Exception{
        boolean deleted = fs.delete(new Path("demo1"), true);
        
        if (deleted) {
            System.out.println("File deleted successfully.");
        } else {
            System.out.println("Failed to delete file.");
        }

        fs.close();
	}
	/**
	 * test tìm kiếm
	 * @throws Exception
	 */
	@Test
	public void testLs() throws Exception{
		RemoteIterator<LocatedFileStatus> iter = fs.listFiles(new Path("/"), true);
		Thread.sleep(1000);
		while(iter.hasNext()) {
			LocatedFileStatus status = iter.next();
			System.out.println("Block size: " + status.getBlockSize());
			System.out.println("Lenght: " + status.getLen());
			System.out.println("Replication: " + status.getReplication());
			System.out.println("Block Location: "+ Arrays.toString(status.getBlockLocations()));
			System.out.println("-------------------------------------------------------------");
			
		}
		
		fs.close();
	}
	/**
	 * Test đọc dữ liệu
	 * @throws Exception
	 */
	@Test
	public void ReadDataFromHDFS() throws Exception {
		FSDataInputStream in = fs.open(new Path("/demo1"));
		
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
	
		String line = null;
		while((line=br.readLine())!=null) {
			System.out.println(new String(line));
		}
		
		br.close();
		in.close();
		fs.close();
	}
	/**
	 * Test đọc dữ liệu bằng cú pháp seek
	 * @throws Exception
	 */
	@Test
	public void RandomRead() throws Exception {
		FSDataInputStream in = fs.open(new Path("/demo1"));
		
		in.seek(12);
		byte[] buf = new byte[1024];
		in.read(buf);
		
		System.out.println(new String(buf));
		
		in.close();
		fs.close();
	} 
	/**
	 * Test ghi dữ liệu 
	 * @throws Exception
	 */
	@Test
	public void WriteData() throws Exception {
		FSDataOutputStream out = fs.create(new Path("/yy.jpg"),false);
		//C:\chup-anh-dep-bang-dien-thoai-25.jpg
		FileInputStream in = new FileInputStream("C:\\chup-anh-dep-bang-dien-thoai-25.jpg");
		
		byte[] buf = new byte[1024];
		int read = 0;
		while((read = in.read(buf))!=-1) {
			out.write(buf,0,read);	
		}
		in.close();
		out.close();
		fs.close();
		
	}
}
