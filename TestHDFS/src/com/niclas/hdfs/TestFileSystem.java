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

	//�����ļ���--��mkdir -p
	public static void creatDir(){
		conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
			try {
				//��ȡHDFS���ļ�ϵͳʵ��--FileSystem��
				fileSystem = FileSystem.get(conf);
				//URI:hdfs://192.168.33.216:9000������jdbc:mysql://localhost:3306
				System.out.println(fileSystem.getUri());
				boolean b = fileSystem.mkdirs(new Path("/practice"));
				System.out.println(b);
				fileSystem.close();
			} catch (IOException e) {
				e.printStackTrace();
			}	
	}
	
	//ɾ���ļ��� ������Ƿǿ��ļ��У�����2�����ֵtrue
	public static void deletDir() throws IllegalArgumentException, IOException {
		conf = new Configuration();		
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
		fileSystem = FileSystem.get(conf);
		//fileSystem = FileSystem.get(new URI("hdfs://192.168.33.216:9000"), conf, "hadoop");
		System.out.println(fileSystem);		
		boolean delete = fileSystem.delete(new Path("/tmp"),true);
		System.out.println(delete?"ɾ���ɹ�":"ɾ��ʧ��");						
	}
	//�����ļ�
	public static void copyFromLocal() throws IllegalArgumentException, IOException {
		//���ֵ����⣺
		//����HDFS���ȡ��ǰ�ͻ��˵ĵ�¼��Ϣ��HDFS���ļ�Ȩ�޲����������û�д�롣
		/*
		 * ��һ���������޸�HDFS���ļ�Ȩ��Ϊ�����û���д
		 * �ڶ����������ڿͻ��������û���ΪHDFS���ļ�������
		 * �������������ڼ�Ⱥ�������޸�namenode�е�hadoop������hdfs-site.xml�ļ�
		 * ������������
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
		//fileSystemʵ��src:source Դ�ļ��� ����
		//dsc:destination��Ŀ���ļ���HDFS�ļ�ϵͳ
		fileSystem.copyFromLocalFile(new Path("E:/hadoop/output/123.txt"), new Path("/test/123.txt"));
		
	}
	
	//�����ļ�
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
	

	
	//��ӡconf����Ϣ
	public static void confInformation() {
		conf = new Configuration();	
		Iterator<java.util.Map.Entry<String, String>> iterator = conf.iterator();
		while (iterator.hasNext()) { 
			java.util.Map.Entry<String, String> entry = iterator.next();
			System.out.println("key:"+entry.getValue()+"value:"+entry.getValue());
			
		}
	}
	
	//����·���ļ���Ϣ
	public static void listFiles() throws FileNotFoundException, IllegalArgumentException, IOException {
		
		conf = new Configuration();		
		conf.set("fs.defaultFS", "hdfs://192.168.33.216:9000");		
		fileSystem = FileSystem.get(conf);
		RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);
		while (listFiles.hasNext()) {
			  LocatedFileStatus fileInfor = listFiles.next();
			  System.out.println("�ļ����ȣ�"+fileInfor.getLen());
			  System.out.println("�ļ�����������"+fileInfor.getReplication());
			  System.out.println("�ļ�����Ĵ�С��"+fileInfor.getBlockSize());			
		}
	}	
		
}
