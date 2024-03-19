package com.heco.toolkit.hbase.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class TestScanLimit {
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
		String tableName = "testPutTable2";
		 HTable table = new HTable(hbaseConf, tableName);
		Scan scan = new Scan();
		  long start = System.currentTimeMillis();
		  System.out.println("start time = "+start);
		  for(int i=1;i<=100000;i=i+100){
			  System.out.println("��"+i/100+"scan ɨ��");
			  scan.setStartRow(Bytes.toBytes(String.format("%0"+9+"d", i)));
			  scan.setStopRow(Bytes.toBytes(String.format("%0"+9+"d", i+100)));
		  }
	        ResultScanner rs = table.getScanner(scan);
	        long end = System.currentTimeMillis();
	        System.out.println("end time = "+end);
            System.out.println("Scan totol time = "+(end-start+"ms"));
	}
		

}
