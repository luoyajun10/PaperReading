package com.heco.toolkit.hbase.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class TestThreadGet {
	private static Configuration hbaseConf = null;
	public static void main(String[] args) throws IOException {
		Configuration HBASE_CONFIG = new Configuration();
		HBASE_CONFIG.set("hbase.zookeeper.property.clientPort", "2181");
		HBASE_CONFIG.set("hbase.master", "xx.xx.xx.xx:60000");
		HBASE_CONFIG.set("hbase.zookeeper.quorum", "hadoop001");
		hbaseConf = HBaseConfiguration.create(HBASE_CONFIG);
		int startKey = Integer.parseInt(args[0]);
		int numbers = Integer.parseInt(args[1]);
		int threadNum = Integer.parseInt(args[2]);
		long start = System.currentTimeMillis();
		System.out.println("start time = " + start);
		for (int i = 0; i < threadNum; i++) {
			GetThread getThread = new GetThread(hbaseConf, startKey, numbers);
			Thread thread = new Thread(getThread);
			thread.start();
			startKey=startKey+numbers;
			System.out.println("started one thread:" + thread.getId());
		}
		long end = System.currentTimeMillis();
		System.out.println("end time = " + end);
		System.out.println("Scan totol time = " + (end - start)+ "ms");
	}

}
