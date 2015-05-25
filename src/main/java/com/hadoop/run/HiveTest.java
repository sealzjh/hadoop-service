package com.hadoop.run;

import java.io.IOException;
import java.sql.Connection;  
import java.sql.DriverManager;  
import java.sql.ResultSet;  
import java.sql.SQLException;  
import java.sql.Statement; 

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;

public class HiveTest {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://192.168.196.157:10000/default";  
    private static String user = "hive";  
    private static String password = "hivepass";  
    private static String sql = "";  
    private static ResultSet res;
	
	public static void main(String [] args) throws ClassNotFoundException {
		System.out.println("is running hbase");
		
		Connection conn = null;  
        Statement stmt = null;  
        try {  
            conn = getConn();
            stmt = conn.createStatement();
            String tableName = "weijin_supplier";
            //createTable(stmt , tableName);
            //loadData(stmt , tableName);
            //describeTables(stmt, tableName); 
            selectData(stmt, tableName);  
        }catch (SQLException e) {  
            e.printStackTrace();  
            System.exit(1);  
        }
	}
	
	private static void createTable(Statement stmt, String tableName)  
            throws SQLException {  
        sql = "create table "  
                + tableName  
                + " (id int , key int , name string , type int , phone string, shop string)  row format delimited fields terminated by '|'";  
        res = stmt.executeQuery(sql);  
    }  
	
	private static void loadData(Statement stmt, String tableName)  
            throws SQLException {  
        String filepath = "/home/hadoop/supplier-online-01.txt";  
        sql = "load data local inpath '" + filepath + "' into table "  
                + tableName;  
        System.out.println("Running:" + sql);  
        res = stmt.executeQuery(sql);  
    }  
	
	 private static void showTables(Statement stmt, String tableName)  
	            throws SQLException {  
	        sql = "show tables '" + tableName + "'";  
	        System.out.println("Running:" + sql);  
	        res = stmt.executeQuery(sql);  
	        System.out.println("执行 show tables 运行结果:");  
	        if (res.next()) {  
	            System.out.println(res.getString(1));  
	        }  
	    } 
	
	private static void describeTables(Statement stmt, String tableName)  
            throws SQLException {  
        sql = "describe " + tableName;  
        res = stmt.executeQuery(sql);  
        System.out.println("describe table:");  
        while (res.next()) {  
            System.out.println(res.getString(1) + "\t" + res.getString(2));  
        }  
    }  
	
	private static void selectData(Statement stmt, String tableName)  
            throws SQLException {  
        sql = "select * from " + tableName;  
        res = stmt.executeQuery(sql);  
        System.out.println("select data:");  
        while (res.next()) {  
            System.out.println(res.getInt(1) + "\t" + res.getInt(2) + "\t" + res.getString(3) + "\t" + res.getInt(4) + "\t" + res.getString(5) + "\t" + res.getString(6));  
        }  
    }
	
	private static Connection getConn() throws ClassNotFoundException,  
    SQLException {  
		Class.forName(driverName);  
		Connection conn = DriverManager.getConnection(url, user, password);  
		return conn;  
	}  
	
}
