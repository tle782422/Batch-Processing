package org.hadoop.hdfs.serialize;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Book implements Writable {
	 private String SID;
     private String name;
     private String author;
	@Override
	public void write(DataOutput out) throws IOException {
        Text.writeString(out, SID);
        Text.writeString(out, name);
        Text.writeString(out, author);
		
	}
	@Override
	public void readFields(DataInput in) throws IOException {
        SID = Text.readString(in);
        name = Text.readString(in);
        author = Text.readString(in);
	}
	
	public Book() {
		// TODO Auto-generated constructor stub
	}
	
    public Book(String sID, String name, String author) {
		super();
		SID = sID;
		this.name = name;
		this.author = author;
	}
	public String getSID() {
		return SID;
	}
	public void setSID(String sID) {
		SID = sID;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getAuthor() {
		return author;
	}
	public void setAuthor(String author) {
		this.author = author;
	}
	
	// Demo quá trình truyền nhận và lưu trữ dữ liệu trong Hadoop Sử dụng SequenceFile
	public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "1");
        conf.set("dfs.blocksize", "64m");

        // Đường dẫn tới HDFS
        Path outputPath = new Path("hdfs://master:9000/user/hadoop/output/sequencefile");

        // Tạo một đối tượng StudentWritable
        Book book = new Book();
        book.setSID("S001");
        book.setName("abc");
        book.setAuthor("abc");

        // Tạo một SequenceFile để lưu trữ dữ liệu
        // Một số cách khác như Avro, Parquet, ORC, (arrayfile setfile mapfile)
        SequenceFile.Writer.Option keyClass = SequenceFile.Writer.keyClass(LongWritable.class);
        SequenceFile.Writer.Option valueClass = SequenceFile.Writer.valueClass(Book.class);
        SequenceFile.Writer.Option outputPathOption = SequenceFile.Writer.file(outputPath);

        SequenceFile.Writer writer = SequenceFile.createWriter(conf, keyClass, valueClass, outputPathOption);

        // Viết dữ liệu vào SequenceFile dạng (key,value)
        writer.append(new LongWritable(1), book);

        // Đóng writer
        writer.close();

        // Ví dụ đọc
        // Đọc dữ liệu từ SequenceFile
        SequenceFile.Reader.Option filePathOption = SequenceFile.Reader.file(outputPath);
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, filePathOption);

        LongWritable key = new LongWritable();
        Book value = new Book();

        // Đọc dữ liệu từ SequenceFile
        while (reader.next(key, value)) {
            System.out.println("Key: " + key.get() + ", Value: " + value.toString());
        }

        // Đóng reader
        reader.close();
    }
}
