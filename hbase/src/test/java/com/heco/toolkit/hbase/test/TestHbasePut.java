package com.heco.toolkit.hbase.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;

public class TestHbasePut {
	private static Configuration hbaseConf = null;
	
	public static void main(String[] args) {
		Configuration HBASE_CONFIG = new Configuration();
		HBASE_CONFIG.set("hbase.zookeeper.property.clientPort", "2181");
		HBASE_CONFIG.set("hbase.master", "xx.xx.xx.xx:60000");
		HBASE_CONFIG.set("hbase.zookeeper.quorum", "hadoop001");
		hbaseConf = HBaseConfiguration.create(HBASE_CONFIG);
		insert(false,false,1024*1024*24);
	}
	private static void insert(boolean wal,boolean autoFlush,long writerBuffer){
		String tableName = "testPutTable2";
		try { 
            HBaseAdmin hBaseAdmin = new HBaseAdmin(hbaseConf); 
            if (hBaseAdmin.tableExists(tableName)) {// �������Ҫ�����ı���ô��ɾ�����ٴ���                 hBaseAdmin.disableTable(tableName); 
            	hBaseAdmin.disableTable(tableName);
            	hBaseAdmin.deleteTable(tableName); 
                System.out.println(tableName + " is exist,detele...."); 
            } 
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName); 
            tableDescriptor.addFamily(new HColumnDescriptor("column1")); 
            hBaseAdmin.createTable(tableDescriptor); 
            HTablePool pool = new HTablePool(hbaseConf, 1000); 
            List<Put> lp = new ArrayList<Put>();
            long start = System.currentTimeMillis();
            System.out.println("start time = "+start);
            long count = 1000000;
            byte[] buffer = new byte[128];
            Random r = new Random();
            for(int i = 1;i<=count;++i){
            	Put p = new Put(("row"+String.format("%0"+9+"d", i)).getBytes());
            	r.nextBytes(buffer);
            	p.add("column1".getBytes(),null,buffer);
            	lp.add(p);
            	if(i%10000==0){
            		pool.getTable(tableName).put(lp);
            		System.out.println("��"+(i/10000)+"�β���");
            		lp.clear();
            	}
            }
            System.out.println("WAL="+wal+",autoFlush="+autoFlush+",buffer="+writerBuffer+",count="+count);
            long end = System.currentTimeMillis();
            System.out.println("total need time = "+((end-start)*1.0)/1000+"s");
        } catch (MasterNotRunningException e) { 
            e.printStackTrace(); 
        } catch (ZooKeeperConnectionException e) { 
            e.printStackTrace(); 
        } catch (IOException e) { 
            e.printStackTrace(); 
        } 
	}
		

}
