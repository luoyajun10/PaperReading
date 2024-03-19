package com.heco.toolkit.hbase.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class ScanThread extends Thread {
	private Configuration hbaseConf ;
	private int startKey;
	private int numbers;

	public ScanThread(Configuration hbaseConf,int startKey, int numbers) {
		super();
		this.hbaseConf = hbaseConf;
		this.startKey = startKey;
		this.numbers = numbers;
	}

	public void run() {
		String tableName = "testPutTable2";
		HTable table = null;
		try {
			table = new HTable(hbaseConf, tableName);
			Scan scan = new Scan();
			long start = System.currentTimeMillis();
			System.out.println("start time = " + start);
			scan.setStartRow(Bytes.toBytes(String.format("%0" + 9 + "d", startKey)));
			scan.setStopRow(Bytes.toBytes(String.format("%0" + 9 + "d", (startKey + numbers))));
			ResultScanner rs = table.getScanner(scan);
			long end = System.currentTimeMillis();
			System.out.println("end time = " + end);
			FileOutputStream fos = new FileOutputStream(File.separator+"home"+File.separator+"hadoop"+File.separator+"hbaseScan.txt",true);
			OutputStreamWriter osw = new OutputStreamWriter(fos,"UTF-8");
			PrintWriter pw = new PrintWriter(osw);
			pw.println(start+"\t"+end+"\t"+(end - start));
			pw.flush();
			pw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				table = null;
			}
		}
	}

}
