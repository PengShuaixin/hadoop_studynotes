package com.niclas.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;


public class StreamInHDFS {
	FileSystem fileSystem;
	@Before
	public void init() throws IOException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		fileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), conf);		
	}
	//�����ļ�
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
	
	//�ϴ��ļ�
	@Test	
	public void testUpload() throws IllegalArgumentException, IOException {
		FileInputStream fileInputStream = new FileInputStream(new File("E:/hadoop/output/123.txt"));
		FSDataOutputStream fsOutputStream = fileSystem.create(new Path("/input/b.txt"));
		IOUtils.copy(fileInputStream, fsOutputStream);
	}
	
	//�����ȡ�ļ�
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
	
	//��ȡ�ڶ���block
	@Test
	public void ReadBlock() throws IOException{
		FileStatus fileStatus = fileSystem.getFileStatus(new Path("/input/hadoop-2.7.3.tar.gz"));
		BlockLocation[] fileBlockLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
	    BlockLocation blockLocation = fileBlockLocations[1];
	    System.out.println("ƫ����:" + blockLocation.getOffset());
	    System.out.println("����:" + blockLocation.getLength());
	    //��ȡ�ڶ����ƫ�����ͳ���
	    long offset = blockLocation.getOffset();
	    long length = blockLocation.getLength();
	    //��ȡhdfs��������
	    FSDataInputStream open = fileSystem.open(new Path("/input/hadoop-2.7.3.tar.gz"));
	    //��ȡ�����ļ��������
	    FileOutputStream outputStream = new FileOutputStream(new File("F:/Data/block"));
        //ʹ��IOUTils���߽��д���
	    IOUtils.copyLarge(open, outputStream, offset, length);	
	}
	
	public static void listFiles(FileSystem fileSystem) throws FileNotFoundException, IllegalArgumentException, IOException {
		RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);
		while (listFiles.hasNext()) {
			//�õ��ļ���Ϣ
			LocatedFileStatus locat = (LocatedFileStatus) listFiles.next();
			System.out.println(locat.getBlockSize());
			System.out.println(locat.getLen());
			System.out.println(locat.getReplication());		
		}		
	}
}
