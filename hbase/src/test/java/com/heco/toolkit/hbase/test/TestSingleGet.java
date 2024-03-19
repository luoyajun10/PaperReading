package com.heco.toolkit.hbase.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

public class TestSingleGet {
	private static Configuration hbaseConf = null;
	
	public static void main(String[] args) throws IOException {
		Configuration HBASE_CONFIG = new Configuration();
		HBASE_CONFIG.set("hbase.zookeeper.property.clientPort", "2181");
		HBASE_CONFIG.set("hbase.master", "xx.xx.xx.xx:60000");
		HBASE_CONFIG.set("hbase.zookeeper.quorum", "hadoop001");
		hbaseConf = HBaseConfiguration.create(HBASE_CONFIG);
		get();
	}
	private static void get() throws IOException{
		String tableName = "test";
		 HTable table = new HTable(hbaseConf, tableName);
		 String rowKey = "rowkey";
		  long start = System.currentTimeMillis();
	        Get g = new Get(rowKey.getBytes());
	        Result rs = table.get(g);
	        for (KeyValue kv : rs.raw())
	        {
	            System.out.println("--------------------" + new String(kv.getRow()) + "----------------------------");
	            System.out.println("Column Family: " + new String(kv.getFamily()));
	            System.out.println("Column       :" + new String(kv.getQualifier()));
	            System.out.println("value        : " + new String(kv.getValue()));
	        }
	        long end = System.currentTimeMillis();
            System.out.println("total need time = "+(end-start+"ms"));
	}
		

}
