package org.hadoop.hdfs;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

public class HdfsCompression {
	/**
	 * hdfs Client codec demo ghi dữ liệu
	 * @param args
	 * @throws Exception
	 */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "2");
        conf.set("dfs.blocksize", "64m");

        // Khởi tạo FileSystem với URI của HDFS và cấu hình 
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://master:9000/"), conf, "toanle");

        // Chọn codec nén DeflateCodec.class, GzipCodec.class, BZip2Codec.class, LzoCodec.class, Lz4Codec.class, SnappyCodec.class, ... 	
        Class<? extends CompressionCodec> codecClass = DeflateCodec.class;
        CompressionCodec codec = ReflectionUtils.newInstance(codecClass, conf);

        // Tạo một đối tượng CompressionOutputStream để ghi dữ liệu nén vào HDFS
        CompressionOutputStream cos = codec.createOutputStream(fileSystem.create(new Path("/demo1/a.deflate")));

        // Sao chép dữ liệu từ tệp cục bộ vào đầu ra nén
        IOUtils.copyBytes(new FileInputStream("C:/cmdlinux.txt"), cos, 1024);

        // Đóng luồng
        cos.close();
        fileSystem.close();
    }
	/**
	 * local test VM
	 * @throws Exception
	 */
	@Test
	public void testDeflate() throws Exception {
		Configuration conf = new Configuration();
		//GzipCodec.class, BZip2Codec.class, LzoCodec.class, Lz4Codec.class, SnappyCodec.class, ... 		
		Class codecClass = DeflateCodec.class;
		CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
		Compressor comp = CodecPool.getCompressor(codec);
		CompressionOutputStream cos = codec.createOutputStream(new FileOutputStream("/home/ubuntu/codec/b.deflate"),comp);
		IOUtils.copyBytes(new FileInputStream("/home/ubuntu/codec/a.deflate"), cos, 1024);
		cos.close();
	}
	/**
	 * Đọc dữ liệu từ tệp nén để lưu vào máy cục bộ
	 * @throws Exception
	 */
	@Test
    public void ReadCompression() throws Exception {
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "1");
        conf.set("dfs.blocksize", "64m");

        // Khởi tạo FileSystem với URI của HDFS và cấu hình
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://master:9000/"), conf, "toanle");

        // Lấy đường dẫn của tệp nén trên HDFS
        Path inputPath = new Path("/demo1/a.deflate");

        // Chọn codec nén (ở đây là DeflateCodec)
        Class<? extends CompressionCodec> codecClass = DeflateCodec.class;
        CompressionCodec codec = ReflectionUtils.newInstance(codecClass, conf);

        // Tạo một đối tượng CompressionInputStream để đọc dữ liệu nén từ HDFS
        CompressionInputStream cis = codec.createInputStream(fileSystem.open(inputPath));

        // Mở một luồng đến tệp đầu ra trên hệ thống cục bộ
        FileOutputStream fos = new FileOutputStream("C:/cmdlinux_decompressed.txt");

        // Sao chép dữ liệu từ đầu vào nén vào tệp đầu ra giải nén
        IOUtils.copyBytes(cis, fos, 1024);

        // Đóng luồng
        fos.close();
        cis.close();
        fileSystem.close();
    }

}
