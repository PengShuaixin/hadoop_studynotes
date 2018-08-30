package com.niclas.provinceflow;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



/*
 *ִ�ж��job
 */
public class FlowCount2 {
	static FlowBean flowBean = new FlowBean();
	static Text text = new Text();
	//��һ�δ����ļ�
	static class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context)
				throws IOException, InterruptedException {
			//�߼�����
			String[] values = value.toString().split("\t");
			String phoneNumber = values[1];
			String upFlowStr = values[values.length - 3];
			String downFlowStr = values[values.length - 2];
			flowBean.set(Integer.parseInt(upFlowStr), Integer.parseInt(downFlowStr));
			//д���ļ���
			context.write(new Text(phoneNumber), flowBean);	
		}
		
	}
	//<135180123345,<flowbean1,flowbean2,flowbean3>>
	//<135180123345,<flowbean1,flowbean2,flowbean3>>
	//<135180123345,<��Flowbean>>
	static class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

		@Override
		protected void reduce(Text text, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context)
				throws IOException, InterruptedException {
			int sumUPFlow = 0;
			int sumDownFlow = 0;
			for (FlowBean flowBean : values) {
				sumUPFlow += flowBean.getUpflow();
				sumDownFlow += flowBean.getDownFlow();
			}
		flowBean.set(sumUPFlow,sumDownFlow);
		context.write(text, flowBean);
		}
		
	}
	
	//�ڶ��δ���
	static class SortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, Text>.Context context)
				throws IOException, InterruptedException {
			String[] values = value.toString().split("\t");
			String phoneNumber = values[0];
			String upFlow = values[1];
			String downFlow = values[2];
			flowBean.set(Integer.parseInt(upFlow),Integer.parseInt(downFlow));
			text.set(phoneNumber);
			context.write(flowBean,text);
		}

		
	}
	static class SortReducer extends Reducer<FlowBean, Text, Text, FlowBean>{

		@Override
		protected void reduce(FlowBean key, Iterable<Text> value, Reducer<FlowBean, Text, Text, FlowBean>.Context context)
				throws IOException, InterruptedException {
			Iterator<Text> iterator = value.iterator();
			context.write(iterator.next(), key);
		}
		
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Path path = new Path("D:/tmp");
		Job job1 = Job.getInstance(conf, "flow");
		//��������job��jar��
		job1.setJarByClass(FlowCount2.class);
		//����Mapper��Reducer
		job1.setMapperClass(FlowMapper.class);
		job1.setReducerClass(FlowReducer.class);
		//��ʽ������Map���������
		job1.setMapOutputKeyClass(Text.class);
		job1.setOutputValueClass(FlowBean.class);
		//����Reduce�������
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(FlowBean.class);
		//����job�ļ�������
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		//����job�ļ������
		FileOutputFormat.setOutputPath(job1, path);
	
		
		
		Job job2 = Job.getInstance(conf);
		//��������job��jar��
		job2.setJarByClass(FlowCount2.class);
		//����Mapper��Reducer
		job2.setMapperClass(SortMapper.class);
		job2.setReducerClass(SortReducer.class);
		//ָ���Զ������ݷ�����
		job2.setPartitionerClass(ProvincePartitioner.class);
		//ָ����Ӧ��������������reducetask
		job2.setNumReduceTasks(5);
		//����Map���������
		job2.setMapOutputKeyClass(FlowBean.class);
		job2.setMapOutputValueClass(Text.class);
		//����Reduce�������
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(FlowBean.class);
		//����job�ļ�������
		FileInputFormat.addInputPath(job2, path);
		//����job�ļ������
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
						
		//�ύjobִ��
		//job.submit();
		if (job1.waitForCompletion(true)) {
			boolean falag = job2.waitForCompletion(true);
			System.out.println(falag?"ִ�гɹ�":"ִ��ʧ��");
		}else {
			System.out.println("ִ��ʧ��-1");
		}
		

		
	}
}
