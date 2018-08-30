package com.niclas.flow;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Flow {
	//错误：在MapReduce框架中处理的数据类型应该是可写的，目前Flowbean没有看到“可写”
	//解决方案：让FloeBean实现可写，即满足hadoop中可序列化
	//在MapReduce框架中，如果自定义的类型作为key，那么需要实现WritableComparable
	//在MapReduce框架中，如果自定义的类型作为value，那么需要实现Writable接口
	static class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//逻辑处理
			String[] values = value.toString().split("\t");
			String phoneNumber = values[1];
			String upFlowStr = values[values.length - 3];
			String downFlowStr = values[values.length - 2];
			FlowBean flowBean = new FlowBean(Integer.parseInt(upFlowStr),Integer.parseInt(downFlowStr));
			//写到文件中
			context.write(new Text(phoneNumber), flowBean);
		}
	}
	//<12777454447,<flowbean1,floebean2,flowbean3>>
	static class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean>{
		@Override
		protected void reduce(Text text, Iterable<FlowBean> values, Context context)
				throws IOException, InterruptedException {
				int sumUpFlow = 0;
				int sumDownFlow = 0;
				for (FlowBean flowBean : values) {
					sumUpFlow += flowBean.getUpFlow();
					sumDownFlow +=flowBean.getDownFlow();
					
				}
				FlowBean sumFlowBean = new FlowBean(sumUpFlow,sumDownFlow);
				context.write(text, sumFlowBean);
		}
	}	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Flow.class);
		job.setMapperClass(FlowMapper.class);
		job.setReducerClass(FlowReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		boolean flag = job.waitForCompletion(true);
		System.out.println(flag?"执行成功":"执行失败");
	}

}
