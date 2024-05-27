package org.hadoop.hdfs.sample;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

// SAMPLE WORD COUNT
public class HdfsWordcount {
	public static void main(String[] args) throws Exception{
			
		Properties props = new Properties();
		props.load(HdfsWordcount.class
				.getClassLoader()
				.getResourceAsStream("job.properties"));
		Class<?> mapper_class = Class.forName(props.getProperty("MAPPER_CLASS"));
		Mapper mapper =  (Mapper) mapper_class.newInstance();
		
		Context context = new Context();
		
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://master:9000/"), new Configuration(), "toanle");
		RemoteIterator<LocatedFileStatus> iter = fileSystem.listFiles(new Path("/wordcount/input/"), false);
		
		while(iter.hasNext()) {
			LocatedFileStatus file = iter.next();
			FSDataInputStream in = fileSystem.open(file.getPath());
			
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			
			String line = null;
			while((line=br.readLine())!=null) {
				mapper.map(line, context);			
				
			}
			br.close();
			in.close();
		}
		
		
		HashMap<Object, Object> contextMap = context.getContextMap();
		
		Path outPath = new Path("/wordcount/output/");
		if(!fileSystem.exists(outPath)) {
			fileSystem.mkdirs(outPath);
		}
		
		FSDataOutputStream out = fileSystem.create(new Path("/wordcount/output/res.dat"));
		
		Set<Entry<Object, Object>> entrySet = contextMap.entrySet();
		for(Entry<Object, Object> entry : entrySet) {
			out.write((entry.getKey().toString() +"\t"+ entry.getValue()+"\n").getBytes());
		}
		out.close();
		fileSystem.close();
		
		System.out.println("OK!!!");
	}
}
