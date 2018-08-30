package com.niclas.reducejoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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


public class TestOrderProject {
	//order文件和product文件--》一个split-->2个split
	static class OrderMapper extends Mapper<LongWritable, Text, Text, OrderJoinBean>{
		OrderJoinBean orderJoinBean = new OrderJoinBean();
		Text text = new Text();
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			//构造OrderJoinBeanj对象
			//确定文件来源--》
			FileSplit split = (FileSplit) context.getInputSplit();
			Path path = split.getPath();
			//E:\hadoop\output\Order.txt-->存在问题
			String[] values = value.toString().split("\t");
			String order_id = "order";
			String date = "date";
			String name = "name";
			String category_id = "category";
			String price = "price";
			String pid = "pid";
			if ("Order.txt".equals(path.getName())) {
				//从Order数据中取出order_id, date
				order_id = values[0];
				date = values[1];
				pid = values[2];
			}else {
				//从Product数据中取出name, category_id, price
				name = values[1];
				category_id = values[2];
				price = values[3];
				pid = values[0];
			}
			orderJoinBean.set(order_id, date, name, category_id, price);
			text.set(pid);
				context.write(text, orderJoinBean);
		}
		
	}
	//走order文件
	//<p1001,<<001,2018829,p1001,null,null,null>，OrderJoinBean>>
	//<p1002,<<002,2017569,p1002,null,null,null>，<null,null,华为，ct80,5>>
	//走product文件
	//<p1001,<<001,2018829,null,null,null>，<002,323535,null,null,null>，<null,null,小米，cto5,5>>
	//<p1002,<<002,2017569,null,null,null>，<null,null,华为，ct80,5>>
	static class OrderReducer extends Reducer<Text, OrderJoinBean, NullWritable, OrderJoinBean>{
		OrderJoinBean oJB = new OrderJoinBean();
		List<OrderJoinBean> list = new ArrayList<OrderJoinBean>();
		@Override
		protected void reduce(Text key, Iterable<OrderJoinBean> values,Context context)
				throws IOException, InterruptedException {
			//Order中pid和Product中id相同的那些OrderJoinBean
			String order_id = null;
			String date = null;
			String name = null;
			String category_id = null;
			String price = null;
			for (OrderJoinBean orderJoinBean : values) {
				if (orderJoinBean.getOrder_id().equals("order")) {
					name = orderJoinBean.getName();
					category_id = orderJoinBean.getCategory_id();
					price = orderJoinBean.getPrice();
				}else {
					order_id = orderJoinBean.getOrder_id();
					date = orderJoinBean.getDate();
					oJB.set(order_id, date, name, category_id, price);
					list.add(oJB);
				}
			}
			//遍历list写入到文件当中
			for (OrderJoinBean orderJoinBean : list) {
				orderJoinBean.setName(name);
				orderJoinBean.setCategory_id(category_id);
				orderJoinBean.setPrice(price);
				context.write(NullWritable.get(), orderJoinBean);//返回对象实例
			}
			
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		
		job.setJarByClass(TestOrderProject.class);
		job.setMapperClass(OrderMapper.class);
		job.setReducerClass(OrderReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(OrderJoinBean.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(OrderJoinBean.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean waitForCompletion = job.waitForCompletion(true);
		System.out.println(waitForCompletion?"执行成功":"执行失败");
	}

}
