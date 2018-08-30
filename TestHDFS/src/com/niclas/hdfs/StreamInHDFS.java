package com.niclas.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;


public class StreamInHDFS {
	FileSystem fileSystem;
	@Before
	public void init() throws IOException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		fileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), conf);		
	}
	@Test
	public  void testDownload() throws IllegalArgumentException, IOException {
		//1、先从hdfs上获取文件--》读--输入流
		FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/test/123.txt"));
		//2、获取本地文件的输出流--》写--输出流
		FileOutputStream outputStream = new FileOutputStream(new File("E:/hadoop/input/download.txt"));
		//使用IOUtils
		IOUtils.copy(fsDataInputStream, outputStream);
		//IOUtils.copyBytes(fsDataInputStream, outputStream, 4096);
	}
	
	@Test
	
	public void testUpload() throws IllegalArgumentException, IOException {
		FileInputStream fileInputStream = new FileInputStream(new File("E:/hadoop/output/123.txt"));
		FSDataOutputStream fsOutputStream = fileSystem.create(new Path("/input/b.txt"));
		IOUtils.copy(fileInputStream, fsOutputStream);
	}
	@Test
	public void RandomDownload() throws IllegalArgumentException, IOException {
		//1.获取hdfs输入流
		FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/test/123.txt"));
		//2.定位到特定的偏移量
		fsDataInputStream.seek(10);
		//3.获取本地文件输出流
		FileOutputStream fileOutputStream = new FileOutputStream(new File("E:/hadoop/input/123random.txt"));
		//4.使用IOUtiles工具
		IOUtils.copy(fsDataInputStream, fileOutputStream);
		
	}
	
	@Test
	public void ReadBlock() throws IllegalArgumentException, IOException {
		//读取第二块block信息
		FileStatus fileStatus = fileSystem.getFileStatus(new Path("/test/123.txt"));
		BlockLocation[] fileBlockLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());		
		//找到第二块block
		BlockLocation blockLocation = fileBlockLocations[0];
		//获取第二块block的偏移量及长度
		long offset = blockLocation.getOffset();
		long length = blockLocation.getLength();
		//获取hdfs文件系统的输入流
		FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/test/123.txt"));
		//获取本地文件系统的输出流
		FileOutputStream fileOutputStream = new FileOutputStream(new File("E:/hadoop/input/123block.txt"));
		IOUtils.copy(fsDataInputStream, fileOutputStream);
		
		
	}
		
}
