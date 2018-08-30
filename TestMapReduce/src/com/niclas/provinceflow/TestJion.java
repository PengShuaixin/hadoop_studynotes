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
	
	//Mapper
	/*KEYIN:LongWritable:偏移量
	 *VALUEIN：Text：一行文本的内容
	 *KEYOUT:FlowBean
	 *VALUEOUT:Text手机号
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
    
    //第一个job的配置
    Job job1 = new Job(conf,"join1");
    job1.setJarByClass(MODEL.class); 

      job1.setMapperClass(Map_First.class); 
      job1.setReducerClass(Reduce_First.class); 

    job1.setMapOutputKeyClass(Text.class);//map阶段的输出的key 
    job1.setMapOutputValueClass(IntWritable.class);//map阶段的输出的value 
  
    job1.setOutputKeyClass(Text.class);//reduce阶段的输出的key 
    job1.setOutputValueClass(IntWritable.class);//reduce阶段的输出的value 
    
    //加入控制容器 
    ControlledJob ctrljob1=new  ControlledJob(conf); 
    ctrljob1.setJob(job1); 
    //job1的输入输出文件路径
    FileInputFormat.addInputPath(job1, new Path(args[0])); 
      FileOutputFormat.setOutputPath(job1, new Path(args[1])); 

      //第二个作业的配置
    	Job job2=new Job(conf,"Join2"); 
      job2.setJarByClass(MODEL.class); 
      
      job2.setMapperClass(Map_Second.class); 
     job2.setReducerClass(Reduce_Second.class); 
     
    job2.setMapOutputKeyClass(Text.class);//map阶段的输出的key 
    job2.setMapOutputValueClass(IntWritable.class);//map阶段的输出的value 

    job2.setOutputKeyClass(Text.class);//reduce阶段的输出的key 
    job2.setOutputValueClass(IntWritable.class);//reduce阶段的输出的value 

    //作业2加入控制容器 
    ControlledJob ctrljob2=new ControlledJob(conf); 
    ctrljob2.setJob(job2); 
  
     //设置多个作业直接的依赖关系 
       //如下所写： 
     //意思为job2的启动，依赖于job1作业的完成 
  
    ctrljob2.addDependingJob(ctrljob1); 
    
    //输入路径是上一个作业的输出路径，因此这里填args[1],要和上面对应好
    FileInputFormat.addInputPath(job2, new Path(args[1]));
    
    //输出路径从新传入一个参数，这里需要注意，因为我们最后的输出文件一定要是没有出现过得
    //因此我们在这里new Path(args[2])因为args[2]在上面没有用过，只要和上面不同就可以了
    FileOutputFormat.setOutputPath(job2,new Path(args[2]) );

    //主的控制容器，控制上面的总的两个子作业 
    JobControl jobCtrl=new JobControl("myctrl"); 
  
    //添加到总的JobControl里，进行控制
    jobCtrl.addJob(ctrljob1); 
    jobCtrl.addJob(ctrljob2); 


    //在线程启动，记住一定要有这个
    Thread  t=new Thread(jobCtrl); 
    t.start(); 

    while(true){ 

    if(jobCtrl.allFinished()){//如果作业成功完成，就打印成功作业的信息 
    System.out.println(jobCtrl.getSuccessfulJobList()); 
    jobCtrl.stop(); 
    break; 
    }
    }
		 * 
		 */
		 
		

	}

}
