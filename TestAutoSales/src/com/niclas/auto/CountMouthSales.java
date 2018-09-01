package com.niclas.auto;
/*
 * 1.2输出每个月汽车销售数量的百分比
 */
import java.io.IOException;
import java.text.DecimalFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CountMouthSales {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", " "); 
		Job job = Job.getInstance(conf);
		job.setJarByClass(CountUse.class);
		
		job.setMapperClass(CountMapper.class);
		job.setReducerClass(CountReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.out.println(job.waitForCompletion(true)?"执行成功":"执行失败");
	}
	static int num = 0;
	public static class CountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] values = value.toString().split("\t");
			if (values.length >= 11) {
				if (!values[1].isEmpty()&&!values[11].isEmpty()) {
					context.write(new Text(values[1]),new IntWritable(Integer.parseInt(values[11])));
					num++;
				}
			}
			
		}
		
	}	
	public static class CountReduce extends Reducer<Text, IntWritable, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			double count = 0;
			for (IntWritable value : values) {
				count += value.get();
			}
			double avg = count/num;
			DecimalFormat decimalFormat = new DecimalFormat("0.00%");
			context.write(key, new Text("月的销售比为：" + decimalFormat.format(avg)));
		}		
	}
}
