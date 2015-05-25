package com.hadoop.run;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class HdfsTest {
	
	public static void main(String [] args) throws IOException {
		System.out.println("is runing");
		
		listDir("hdfs://192.168.196.158:9000/user/hadoop/input/hadoop");
		//createFile("hdfs://192.168.196.158:9000/user/hadoop/input1/test4.txt");
		//readFile("hdfs://192.168.196.158:9000/user/hadoop/input1/test3.txt");
		//uploadFile("hdfs://192.168.196.158:9000/user/hadoop/input1/deliver_fee_rule_index.sql");
	}
	
	//list file
	public static void listDir(String filePath) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl",	org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl",	org.apache.hadoop.fs.LocalFileSystem.class.getName());
		FileSystem fs = FileSystem.get(URI.create(filePath) , conf);
		FileStatus[] fileList = fs.listStatus(new Path(filePath));
		
		for(FileStatus item : fileList) {
			System.out.println("name:"+item.getPath().getName()+"" +
					"\t\t size:"+item.getLen());
		}
		
		fs.close();
	}
	
	//create file
	public static void createFile(String filePath) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl",	org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl",	org.apache.hadoop.fs.LocalFileSystem.class.getName());
		FileSystem fs = FileSystem.get(URI.create(filePath) , conf);
		
		FSDataOutputStream os = fs.create(new Path(filePath));
		
		byte[] buff = "hello world!".getBytes();
		
		os.write(buff , 0 , buff.length);
		os.close();
		fs.close();
	}
	
	//upload file
	public static void uploadFile(String filePath) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl",	org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl",	org.apache.hadoop.fs.LocalFileSystem.class.getName());
		FileSystem fs = FileSystem.get(conf);
		
		fs.copyFromLocalFile(new Path("/Users/alan/deliver_fee_rule_index.sql"), new Path(filePath));
		FileStatus files[] = fs.listStatus(new Path(filePath));  
        for (FileStatus file : files) {  
            System.out.println(file.getPath());  
        }  
		fs.close();
	}
	
	//read file
	public static void readFile(String filePath) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl",	org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl",	org.apache.hadoop.fs.LocalFileSystem.class.getName());
		FileSystem fs = FileSystem.get(URI.create(filePath) , conf);
		
		FSDataInputStream hdfsinStream = fs.open(new Path(filePath));
		InputStreamReader isr = new InputStreamReader(hdfsinStream);
		BufferedReader br = new BufferedReader(isr);
		String str = "";
		while((str = br.readLine()) != null) {
			System.out.println(str);
		}
		
		br.close();
		isr.close();
		hdfsinStream.close();
	}
	
}