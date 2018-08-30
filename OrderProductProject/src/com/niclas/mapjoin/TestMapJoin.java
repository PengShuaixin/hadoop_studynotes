package com.niclas.mapjoin;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TestMapJoin {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		Job job = Job .getInstance(configuration,"MapJoin");
		job.setJarByClass(TestMapJoin.class);
		job.setMapperClass(MapJionMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		//利用job对象，将小表product分发到各个maptask中
		Path path = new Path(args[0]);
		URI uri = path.toUri();	
		job.addCacheFile(uri);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.out.println(job.waitForCompletion(true)?"执行成功":"执行失败");
	}
	
	static class MapJionMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		//在map方法执行之前执行一次
		//在setup方法中把数据product加载进来
		HashMap<String, ProductBean> prouductMap = new HashMap<String, ProductBean>();
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			FileReader fileReader = new FileReader("product.txt");
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String line;
			while (null != (line = bufferedReader.readLine())) {
				String[] values = line.split("\t");
				//在ProductBean类中建了一个存储数据的组？
				ProductBean productBean = new ProductBean(values);
				String pid = values[0];
				prouductMap.put(pid, productBean);
			}
				fileReader.close();
				bufferedReader.close();
			
		}
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] values = value.toString().split("\t");
			String order_id = values[0];
			String date = values[1];
			String pid = values[2];
			ProductBean productBean = prouductMap.get(pid);
			
			StringBuilder stringBuilder = new StringBuilder();
			stringBuilder.append(order_id);
			stringBuilder.append(date);
			stringBuilder.append(productBean.getName());
			stringBuilder.append(productBean.getCategory_id());
			stringBuilder.append(productBean.getPrice());
			
			context.write(new Text(stringBuilder.toString()), NullWritable.get());
		}
	}


}
