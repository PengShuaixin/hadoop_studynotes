package com.niclas.flow2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.niclas.flow.FlowBean;
import com.niclas.testsort.SortBean;

public class TestJion {
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
	
	//Mapper
	/*KEYIN:LongWritable:ƫ����
	 *VALUEIN��Text��һ���ı�������
	 *KEYOUT:FlowBean
	 *VALUEOUT:Text�ֻ���
	 * <0,"155666 180 180 56">
	 */
	static class SortMapper extends Mapper<LongWritable, Text, SortBean, Text>{
		SortBean flowBean = new SortBean();
		Text text = new Text();
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String[] values = value.toString().split("\\s+");
			String phoneNumber = values[0];
			String  upFlow = values[1];
			String downFlow = values[2];	
			flowBean.set(Integer.parseInt(upFlow),Integer.parseInt(downFlow));
			text.set(phoneNumber);
			context.write(flowBean,text);
						
		}
	}
	//Reducer
	static class SortReducer extends Reducer<SortBean, Text, Text, SortBean>{
		@Override
		protected void reduce(SortBean key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
				Iterator<Text> iterator = value.iterator();
				context.write(iterator.next(), key);
		}
	}
	public static void main(String[] args) {
		/*
		 * JobConf conf = new JobConf(MODEL.class);
    
    //��һ��job������
    Job job1 = new Job(conf,"join1");
    job1.setJarByClass(MODEL.class); 

      job1.setMapperClass(Map_First.class); 
      job1.setReducerClass(Reduce_First.class); 

    job1.setMapOutputKeyClass(Text.class);//map�׶ε������key 
    job1.setMapOutputValueClass(IntWritable.class);//map�׶ε������value 
  
    job1.setOutputKeyClass(Text.class);//reduce�׶ε������key 
    job1.setOutputValueClass(IntWritable.class);//reduce�׶ε������value 
    
    //����������� 
    ControlledJob ctrljob1=new  ControlledJob(conf); 
    ctrljob1.setJob(job1); 
    //job1����������ļ�·��
    FileInputFormat.addInputPath(job1, new Path(args[0])); 
      FileOutputFormat.setOutputPath(job1, new Path(args[1])); 

      //�ڶ�����ҵ������
    	Job job2=new Job(conf,"Join2"); 
      job2.setJarByClass(MODEL.class); 
      
      job2.setMapperClass(Map_Second.class); 
     job2.setReducerClass(Reduce_Second.class); 
     
    job2.setMapOutputKeyClass(Text.class);//map�׶ε������key 
    job2.setMapOutputValueClass(IntWritable.class);//map�׶ε������value 

    job2.setOutputKeyClass(Text.class);//reduce�׶ε������key 
    job2.setOutputValueClass(IntWritable.class);//reduce�׶ε������value 

    //��ҵ2����������� 
    ControlledJob ctrljob2=new ControlledJob(conf); 
    ctrljob2.setJob(job2); 
  
     //���ö����ҵֱ�ӵ�������ϵ 
       //������д�� 
     //��˼Ϊjob2��������������job1��ҵ����� 
  
    ctrljob2.addDependingJob(ctrljob1); 
    
    //����·������һ����ҵ�����·�������������args[1],Ҫ�������Ӧ��
    FileInputFormat.addInputPath(job2, new Path(args[1]));
    
    //���·�����´���һ��������������Ҫע�⣬��Ϊ������������ļ�һ��Ҫ��û�г��ֹ���
    //�������������new Path(args[2])��Ϊargs[2]������û���ù���ֻҪ�����治ͬ�Ϳ�����
    FileOutputFormat.setOutputPath(job2,new Path(args[2]) );

    //���Ŀ�������������������ܵ���������ҵ 
    JobControl jobCtrl=new JobControl("myctrl"); 
  
    //��ӵ��ܵ�JobControl����п���
    jobCtrl.addJob(ctrljob1); 
    jobCtrl.addJob(ctrljob2); 


    //���߳���������סһ��Ҫ�����
    Thread  t=new Thread(jobCtrl); 
    t.start(); 

    while(true){ 

    if(jobCtrl.allFinished()){//�����ҵ�ɹ���ɣ��ʹ�ӡ�ɹ���ҵ����Ϣ 
    System.out.println(jobCtrl.getSuccessfulJobList()); 
    jobCtrl.stop(); 
    break; 
    }
    }
		 * 
		 */
		 
		

	}

}
