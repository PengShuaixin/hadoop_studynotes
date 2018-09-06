package com.niclas.myoutputformat;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyOutPutFormat extends FileOutputFormat<IntWritable, Text> {

	@Override
	public RecordWriter<IntWritable, Text> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = job.getConfiguration();
		//1.做什么用
		String extension = "";
		//2.getDefaultWorkFile返回一个临时路径，程序执行完，会被销毁
//		Path file = getDefaultWorkFile(job, extension);
		Path file = new Path("D:\\my");
		Path file1 = new Path("D:\\my1");
		FileSystem fs = file.getFileSystem(conf);
		DataOutputStream out =fs.create(file,false);
		DataOutputStream out1 =fs.create(file1,false);
		return new MyRecordWriter(out,out1);
	}
	//自定义一个RecordWriter:FSDataOutputStream,keyValueSeparator
	static class MyRecordWriter extends RecordWriter<IntWritable, Text>{
		private DataOutputStream out;
		private DataOutputStream out1;
		private static final String utf8 = "GBK";
		private  static final byte[] keyValueSeparator = " ".getBytes();
		private static final byte[] newline ="\n".getBytes();
		public MyRecordWriter(DataOutputStream out,DataOutputStream out1) {
			super();
			this.out = out;
			this.out1 = out1;
		}
        //3.write写完之后的效果
		@Override
		public void write(IntWritable key, Text value) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			if (value.toString().endsWith("*")) {
				boolean nullKey = key == null;
			    boolean nullValue = value == null;
			      if (nullKey && nullValue) {
			        return;
			      }
			      if (!nullKey) {
			        writeObject(key,out);
			      }
			      if (!(nullKey || nullValue)) {
			        out.write(keyValueSeparator);
			      }
			      if (!nullValue) {
			        writeObject(value.toString().substring(0, value.toString().lastIndexOf("*")),out);
			      }
			      out.write(newline);
			}else {
				boolean nullKey = key == null;
			    boolean nullValue = value == null;
			      if (nullKey && nullValue) {
			        return;
			      }
			      if (!nullKey) {
			        writeObject(key,out1);
			      }
			      if (!(nullKey || nullValue)) {
			        out1.write(keyValueSeparator);
			      }
			      if (!nullValue) {
			        writeObject(value.toString().substring(0, value.toString().lastIndexOf("+")),out1);
			      }
			      out1.write(newline);
			}
			
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			out.close();
			out1.close();
		}
		public void writeObject(Object o,DataOutputStream stream) throws IOException {
			if (o instanceof Text) {
		        Text to = (Text) o;
		        stream.write(to.getBytes(), 0, to.getLength());
		      } else {
		    	  stream.write(o.toString().getBytes(utf8));
		      }
		}
	}
	

}




















