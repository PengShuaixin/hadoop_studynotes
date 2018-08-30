package com.niclas.hdfs;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class TestFileSystem {
	
	static Configuration conf = null;
	static FileSystem fileSystem = null ;
	
	public static void main(String[] args) throws IllegalArgumentException, IOException{
		//creatDir();
		//deletDir();
		copyFromLocal();
		//confInformation();
		//copyToLocal();
		//listFiles();
	}

	//创建文件夹--》mkdir -p
	public static void creatDir(){
		conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
			try {
				//获取HDFS中文件系统实例--FileSystem类
				fileSystem = FileSystem.get(conf);
				//URI:hdfs://192.168.33.216:9000类似于jdbc:mysql://localhost:3306
				System.out.println(fileSystem.getUri());
				boolean b = fileSystem.mkdirs(new Path("/practice"));
				System.out.println(b);
				fileSystem.close();
			} catch (IOException e) {
				e.printStackTrace();
			}	
	}
	
	//删除文件夹 ，如果是非空文件夹，参数2必须给值true
	public static void deletDir() throws IllegalArgumentException, IOException {
		conf = new Configuration();		
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
		fileSystem = FileSystem.get(conf);
		//fileSystem = FileSystem.get(new URI("hdfs://192.168.33.216:9000"), conf, "hadoop");
		System.out.println(fileSystem);		
		boolean delete = fileSystem.delete(new Path("/tmp"),true);
		System.out.println(delete?"删除成功":"删除失败");						
	}
	//复制文件
	public static void copyFromLocal() throws IllegalArgumentException, IOException {
		//出现的问题：
		//发现HDFS会获取当前客户端的登录信息，HDFS中文件权限不允许其它用户写入。
		/*
		 * 第一个方案：修改HDFS中文件权限为其它用户可写
		 * 第二个方案：在客户端设置用户名为HDFS中文件所有者
		 * 第三个方案：在集群环境中修改namenode中的hadoop环境中hdfs-site.xml文件
		 * 加上如下内容
		 * 
		<property>
  			<name>dfs.permissions.enabled</name>
   			<value>false </value>
		</property>

		 */
		conf = new Configuration();		
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
		fileSystem = FileSystem.get(conf);
		//fileSystem = FileSystem.get(new URI("hdfs://192.168.33.216:9000"), conf, "hadoop");
		System.out.println(fileSystem);
		//fileSystem实例src:source 源文件： 本地
		//dsc:destination：目标文件：HDFS文件系统
		fileSystem.copyFromLocalFile(new Path("E:/hadoop/output/123.txt"), new Path("/test/123.txt"));
		
	}
	
	//下载文件
	public static void copyToLocal() throws IllegalArgumentException, IOException {		
		conf = new Configuration();		
		conf.set("fs.defaultFS", "hdfs://192.168.33.216:9000");
		fileSystem = FileSystem.get(conf);
		//fileSystem = FileSystem.get(new URI("hdfs://192.168.33.216:9000"), conf, "hadoop");
		System.out.println(fileSystem);
		//fileSystem.copyToLocalFile(new Path("/input/users.txt"),new Path("E:/hadoop/input"));
		//fileSystem.copyToLocalFile(false, new Path("/input/users.txt"), new Path("E:/hadoop/input"));	
		fileSystem.copyToLocalFile(false, new Path("/input/users.txt"), new Path("E:/hadoop/input"),true);
	}
	

	
	//打印conf的信息
	public static void confInformation() {
		conf = new Configuration();	
		Iterator<java.util.Map.Entry<String, String>> iterator = conf.iterator();
		while (iterator.hasNext()) { 
			java.util.Map.Entry<String, String> entry = iterator.next();
			System.out.println("key:"+entry.getValue()+"value:"+entry.getValue());
			
		}
	}
	
	//遍历路径文件信息
	public static void listFiles() throws FileNotFoundException, IllegalArgumentException, IOException {
		
		conf = new Configuration();		
		conf.set("fs.defaultFS", "hdfs://192.168.33.216:9000");		
		fileSystem = FileSystem.get(conf);
		RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);
		while (listFiles.hasNext()) {
			  LocatedFileStatus fileInfor = listFiles.next();
			  System.out.println("文件长度："+fileInfor.getLen());
			  System.out.println("文件副本个数："+fileInfor.getReplication());
			  System.out.println("文件块儿的大小："+fileInfor.getBlockSize());			
		}
	}	
		
}
