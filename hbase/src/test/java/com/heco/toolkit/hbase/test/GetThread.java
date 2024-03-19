package com.heco.toolkit.hbase.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class GetThread extends Thread {
	private Configuration hbaseConf ;
	private long startKey;
	private int numbers;

	public GetThread(Configuration hbaseConf,long startKey, int numbers) {
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
			long start = System.currentTimeMillis();
			FileOutputStream fos = new FileOutputStream(File.separator+"home"+File.separator+"hadoop"+File.separator+"hbaseGet.txt",true);
			OutputStreamWriter osw = new OutputStreamWriter(fos,"UTF-8");
			PrintWriter pw = new PrintWriter(osw);
			for(int i=0;i<numbers;i++){
				long rowKey = (long) (Math.random()*numbers+startKey);
				Get g = new Get(Bytes.toBytes(String.format("%0" + 9 + "d", startKey)));
				Result rs = table.get(g);
			}
			long end = System.currentTimeMillis();
			System.out.println("end time = " + end);
			
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
