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
	//������MapReduce����д������������Ӧ���ǿ�д�ģ�ĿǰFlowbeanû�п�������д��
	//�����������FloeBeanʵ�ֿ�д��������hadoop�п����л�
	//��MapReduce����У�����Զ����������Ϊkey����ô��Ҫʵ��WritableComparable
	//��MapReduce����У�����Զ����������Ϊvalue����ô��Ҫʵ��Writable�ӿ�
	static class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//�߼�����
			String[] values = value.toString().split("\t");
			String phoneNumber = values[1];
			String upFlowStr = values[values.length - 3];
			String downFlowStr = values[values.length - 2];
			FlowBean flowBean = new FlowBean(Integer.parseInt(upFlowStr),Integer.parseInt(downFlowStr));
			//д���ļ���
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
		System.out.println(flag?"ִ�гɹ�":"ִ��ʧ��");
	}

}
