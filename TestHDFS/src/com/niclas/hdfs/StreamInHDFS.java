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
		//1���ȴ�hdfs�ϻ�ȡ�ļ�--����--������
		FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/test/123.txt"));
		//2����ȡ�����ļ��������--��д--�����
		FileOutputStream outputStream = new FileOutputStream(new File("E:/hadoop/input/download.txt"));
		//ʹ��IOUtils
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
		//1.��ȡhdfs������
		FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/test/123.txt"));
		//2.��λ���ض���ƫ����
		fsDataInputStream.seek(10);
		//3.��ȡ�����ļ������
		FileOutputStream fileOutputStream = new FileOutputStream(new File("E:/hadoop/input/123random.txt"));
		//4.ʹ��IOUtiles����
		IOUtils.copy(fsDataInputStream, fileOutputStream);
		
	}
	
	@Test
	public void ReadBlock() throws IllegalArgumentException, IOException {
		//��ȡ�ڶ���block��Ϣ
		FileStatus fileStatus = fileSystem.getFileStatus(new Path("/test/123.txt"));
		BlockLocation[] fileBlockLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());		
		//�ҵ��ڶ���block
		BlockLocation blockLocation = fileBlockLocations[0];
		//��ȡ�ڶ���block��ƫ����������
		long offset = blockLocation.getOffset();
		long length = blockLocation.getLength();
		//��ȡhdfs�ļ�ϵͳ��������
		FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/test/123.txt"));
		//��ȡ�����ļ�ϵͳ�������
		FileOutputStream fileOutputStream = new FileOutputStream(new File("E:/hadoop/input/123block.txt"));
		IOUtils.copy(fsDataInputStream, fileOutputStream);
		
		
	}
		
}
