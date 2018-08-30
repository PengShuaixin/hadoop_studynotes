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
	//下载文件
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
	
	//上传文件
	@Test	
	public void testUpload() throws IllegalArgumentException, IOException {
		FileInputStream fileInputStream = new FileInputStream(new File("E:/hadoop/output/123.txt"));
		FSDataOutputStream fsOutputStream = fileSystem.create(new Path("/input/b.txt"));
		IOUtils.copy(fileInputStream, fsOutputStream);
	}
	
	//随机读取文件
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
	
	//读取第二块block
	@Test
	public void ReadBlock() throws IOException{
		FileStatus fileStatus = fileSystem.getFileStatus(new Path("/input/hadoop-2.7.3.tar.gz"));
		BlockLocation[] fileBlockLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
	    BlockLocation blockLocation = fileBlockLocations[1];
	    System.out.println("偏移量:" + blockLocation.getOffset());
	    System.out.println("长度:" + blockLocation.getLength());
	    //获取第二块的偏移量和长度
	    long offset = blockLocation.getOffset();
	    long length = blockLocation.getLength();
	    //获取hdfs的输入流
	    FSDataInputStream open = fileSystem.open(new Path("/input/hadoop-2.7.3.tar.gz"));
	    //获取本地文件的输出流
	    FileOutputStream outputStream = new FileOutputStream(new File("F:/Data/block"));
        //使用IOUTils工具进行传输
	    IOUtils.copyLarge(open, outputStream, offset, length);	
	}
	
	public static void listFiles(FileSystem fileSystem) throws FileNotFoundException, IllegalArgumentException, IOException {
		RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);
		while (listFiles.hasNext()) {
			//得到文件信息
			LocatedFileStatus locat = (LocatedFileStatus) listFiles.next();
			System.out.println(locat.getBlockSize());
			System.out.println(locat.getLen());
			System.out.println(locat.getReplication());		
		}		
	}
}
