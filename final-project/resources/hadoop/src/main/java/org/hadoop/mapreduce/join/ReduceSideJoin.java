package org.hadoop.mapreduce.join;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReduceSideJoin {
	
	//Join table
	
	public static class ReduceSideJoinMapper extends Mapper<LongWritable, Text, Text, JoinBean>{
		String fileName = null;
		JoinBean bean = new JoinBean();
		Text k = new Text();
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, JoinBean>.Context context)
				throws IOException, InterruptedException {
			FileSplit inputSplit = (FileSplit) context.getInputSplit();
			fileName = inputSplit.getPath().getName();
		}
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, JoinBean>.Context context)
				throws IOException, InterruptedException {
			String[] fields = value.toString().split(",");
			
			if (fileName.startsWith("order")) {
				//bean.set(fields[0], fields[1], fields[2], Integer.parseInt(fields[3]), fields[4], fields[5]);
				bean.set(fields[0], fields[1], "NULL", -1, "NULL","order");
			} else {
				bean.set("NULL",fields[0], fields[1], Integer.parseInt(fields[2].trim()), fields[3], "user");
			}
			k.set(bean.getUserId());
			context.write(k, bean);
		}
		
	}
	
	public static class ReduceSideJoinReducer extends Reducer<Text, JoinBean, JoinBean, NullWritable>{
	    @Override
	    protected void reduce(Text key, Iterable<JoinBean> beans, Context context) throws IOException, InterruptedException {
	        ArrayList<JoinBean> orderList = new ArrayList<>();
	        JoinBean userBean = null; // Khởi tạo userBean là null
	        try {
	            for (JoinBean bean : beans) {
	                if("order".equals(bean.getTableName())) {
	                    JoinBean newBean = new JoinBean();
	                    BeanUtils.copyProperties(newBean, bean);
	                    orderList.add(newBean); // Thêm newBean vào orderList
	                } else {
	                    userBean = new JoinBean(); // Khởi tạo userBean
	                    BeanUtils.copyProperties(userBean, bean); // Copy thuộc tính từ bean sang userBean
	                }   
	            }
	            for (JoinBean bean : orderList) {
	                // Kiểm tra xem userBean có được khởi tạo không
	                if (userBean != null) {
	                    // Nếu userBean đã được khởi tạo, thiết lập các thuộc tính từ userBean vào bean
	                    bean.setUserName(userBean.getUserName());
	                    bean.setUserAge(userBean.getUserAge());
	                    bean.setUserFriend(userBean.getUserFriend());
	                    
	                    context.write(bean, NullWritable.get());
	                } else {
	                    // Nếu userBean chưa được khởi tạo, in ra thông báo hoặc xử lý tùy thuộc vào yêu cầu của bạn
	                    System.err.println("UserBean is not initialized!");
	                }
	            }
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	    }
	}
		
		public static void main(String[] args) throws Exception {
			System.setProperty("hadoop.home.dir", "C:\\test\\winutils-master\\hadoop-3.1.1");
			
			Configuration configuration = new Configuration();
			
			Job job = Job.getInstance(configuration);
			
			job.setJarByClass(ReduceSideJoin.class);
			
			job.setMapperClass(ReduceSideJoinMapper.class);
			job.setReducerClass(ReduceSideJoinReducer.class);
			
			job.setNumReduceTasks(2);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(JoinBean.class);
			
			job.setOutputKeyClass(JoinBean.class);
			job.setOutputValueClass(NullWritable.class);
			

			
			FileInputFormat.setInputPaths(job, new Path("E:\\MAIN\\DuLieuLon(BigData)\\Demo\\testfile\\join"));
			FileOutputFormat.setOutputPath(job, new Path("E:\\MAIN\\DuLieuLon(BigData)\\Demo\\testfile\\join\\output"));
			
			job.waitForCompletion(true);
		}
		
	}
	

