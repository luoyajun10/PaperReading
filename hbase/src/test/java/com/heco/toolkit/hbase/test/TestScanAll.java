package com.heco.toolkit.hbase.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class TestScanAll {
	private static Configuration hbaseConf = null;
	
	public static void main(String[] args) throws IOException {
		Configuration HBASE_CONFIG = new Configuration();
		HBASE_CONFIG.set("hbase.zookeeper.property.clientPort", "2181");
		HBASE_CONFIG.set("hbase.master", "xx.xx.xx.xx:60000");
		HBASE_CONFIG.set("hbase.zookeeper.quorum", "hadoop001");
		hbaseConf = HBaseConfiguration.create(HBASE_CONFIG);
		scan();
	}
	private static void scan() throws IOException{
		String tableName = "test111";
		 HTable table = new HTable(hbaseConf, tableName);
		Scan scan = new Scan();
		  long start = System.currentTimeMillis();
		  System.out.println("start time = "+start);
	        ResultScanner rs = table.getScanner(scan);
	        long end = System.currentTimeMillis();
	        System.out.println("end time = "+end);
            System.out.println("Scan totol time = "+(end-start+"ms"));
	}
		

}
