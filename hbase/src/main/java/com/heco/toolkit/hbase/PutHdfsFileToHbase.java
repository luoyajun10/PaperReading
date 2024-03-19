package com.heco.toolkit.hbase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;

public class PutHdfsFileToHbase extends Thread {
	private String hdfsFilePath;
	private Configuration hbaseConf = null;
	
	public PutHdfsFileToHbase(String hdfsFilePath){
//		System.out.println(hdfsFilePath);
		this.hdfsFilePath = hdfsFilePath;
	}
	
	public void run(){
		System.out.println("Thread of [" + hdfsFilePath + "] start.");
		Configuration HBASE_CONFIG = new Configuration();
		HBASE_CONFIG.set("hbase.zookeeper.property.clientPort", "2181");
		HBASE_CONFIG.set("hbase.master", "10.160.2.248:60000");
		HBASE_CONFIG.set("hbase.zookeeper.quorum", "hadoop001");
		hbaseConf = HBaseConfiguration.create(HBASE_CONFIG);
		
		insert(false,false,1024*1024*24);
//		boolean wal = false;
//		boolean autoFlush = false;
//		long writerBuffer = 1024*1024*24;
//		String tableName = "test123";
//		try { 
//            HBaseAdmin hBaseAdmin = new HBaseAdmin(hbaseConf); 
//            if (hBaseAdmin.tableExists(tableName)) {// �������Ҫ�����ı���ô��ɾ�����ٴ���                 hBaseAdmin.disableTable(tableName); 
//            	hBaseAdmin.disableTable(tableName);
//            	hBaseAdmin.deleteTable(tableName); 
//                System.out.println(tableName + " is exist,detele...."); 
//            } 
//            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName); 
//            tableDescriptor.addFamily(new HColumnDescriptor("column1")); 
//            hBaseAdmin.createTable(tableDescriptor); 
//            HTablePool pool = new HTablePool(hbaseConf, 1000); 
//            List<Put> lp = new ArrayList<Put>();
//    		
//            Configuration hdfsConf = new Configuration();
//    		FileSystem fs = null;
//    		FSDataInputStream in = null;
//    		try {
//    			fs = FileSystem.get(hdfsConf);
//    			in = fs.open(new Path(hdfsFilePath));
//    		} catch (IOException e) {
//    			System.out.println("create hdfs InputStream failed.");
//    		}
//    		
//            long start = System.currentTimeMillis();
//            System.out.println("start time = "+start);
//            String line = "";
//            long count = 1000000;
//            byte[] buffer = new byte[128];
//            Random r = new Random();
//            
////            for(int i = 1;i<=count;++i){
//            int i = 1;
//            String[] lineSp ;
//            while((line = in.readLine()) != null){
//            	i++;
//            	lineSp = line.split(",");
//            	Put p = new Put(lineSp[0].getBytes());
//            	r.nextBytes(buffer);
//            	p.add(lineSp[1].getBytes(),null,buffer);
//            	lp.add(p);
//            	if(i%10000==0){
//            		pool.getTable(tableName).put(lp);
////            		System.out.println("��"+(i/10000)+"�β���");
//            		lp.clear();
//            	}
//            }
//            System.out.println("WAL="+wal+",autoFlush="+autoFlush+",buffer="+writerBuffer+",count="+count);
//            long end = System.currentTimeMillis();
//            System.out.println("total need time = "+((end-start)*1.0)/1000+"s");
//            fs.close();
//        } catch (MasterNotRunningException e) { 
//            e.printStackTrace(); 
//        } catch (ZooKeeperConnectionException e) { 
//            e.printStackTrace(); 
//        } catch (IOException e) { 
//            e.printStackTrace(); 
//        }
	}
	
	
	private void insert(boolean wal,boolean autoFlush,long writerBuffer){
		String tableName = "test111";
		try { 
            HBaseAdmin hBaseAdmin = new HBaseAdmin(hbaseConf); 
//            if (hBaseAdmin.tableExists(tableName)) {
//              hBaseAdmin.disableTable(tableName);
//            	hBaseAdmin.disableTable(tableName);
//            	hBaseAdmin.deleteTable(tableName); 
//                System.out.println(tableName + " is exist,detele...."); 
//            } 
//            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName); 
//            tableDescriptor.addFamily(new HColumnDescriptor("column1")); 
//            hBaseAdmin.createTable(tableDescriptor); 
            HTablePool pool = new HTablePool(hbaseConf, 1000); 
            List<Put> lp = new ArrayList<Put>();
    		
            Configuration hdfsConf = new Configuration();
    		FileSystem fs = null;
    		FSDataInputStream in = null;
    		try {
    			fs = FileSystem.get(hdfsConf);
    			in = fs.open(new Path(hdfsFilePath));
    		} catch (IOException e) {
    			System.out.println("create hdfs InputStream failed.");
    		}
    		InputStreamReader isr = new InputStreamReader(in);
    		BufferedReader br = new BufferedReader(isr);
            long start = System.currentTimeMillis();
            System.out.println("start time = "+start);
            String line = "";
            long count = 1000000;
//            byte[] buffer = new byte[128];
//            Random r = new Random();
            
//            for(int i = 1;i<=count;++i){
            int i = 0;
            String[] lineSp ;
            while((line = br.readLine()) != null){
            	i++;   
//            	if(i<10){
//            		System.out.println(i+" : "+line);
//            	}
//            	if(i > 4999990){
//            		System.out.println(i+" : "+line);
//            	}
            	lineSp = line.split(",");
            	Put p = new Put(lineSp[0].getBytes());
//            	r.nextBytes(buffer);
            	p.add("column1".getBytes(),null,lineSp[1].getBytes());
            	lp.add(p);
            	if(i%10000==0){
            		pool.getTable(tableName).put(lp);
//            		System.out.println("��"+(i/10000)+"�β���");
            		lp.clear();
            	}
            }
            System.out.println("WAL="+wal+",autoFlush="+autoFlush+",buffer="+writerBuffer+",count="+count);
            long end = System.currentTimeMillis();
            System.out.println("total need time = "+((end-start)*1.0)/1000+"s");
//            fs.close();
        } catch (MasterNotRunningException e) { 
            e.printStackTrace(); 
        } catch (ZooKeeperConnectionException e) { 
            e.printStackTrace(); 
        } catch (IOException e) { 
            e.printStackTrace(); 
        } 
	}
}
