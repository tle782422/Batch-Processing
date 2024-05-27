package org.hadoop.hdfs.sample;

// Xử lý logic
public class WordCountMapper implements Mapper{

	@Override
	public void map(String line, Context context) {
		//String[] words = line.split(" "); nếu tất cả đều là chữ thường
		String[] words = line.toLowerCase().split(" ");
		
		for(String word : words) {
			Object value = context.get(word);
			if(null==value) {
				context.write(word,1);
			}else {
				int v = (int)value;
				context.write(words, v+1);
			}
		}
	}

}
