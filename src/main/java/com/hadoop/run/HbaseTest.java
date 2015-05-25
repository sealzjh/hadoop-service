package com.hadoop.run;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class HbaseTest {
	
	
	public static void main(String [] args) throws IOException {
		System.out.println("is running hbase");
		
		String tablename = "users";
		String column = "adress";
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum" , "192.168.196.158,192.168.196.241,192.168.196.226");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		
		HBaseAdmin hAdmin = new HBaseAdmin(conf);  
		  
        if (hAdmin.tableExists(tablename)) {  
            System.out.println("yes");
        } else {
        	System.out.println("no");
        }
		
	}
	
}
