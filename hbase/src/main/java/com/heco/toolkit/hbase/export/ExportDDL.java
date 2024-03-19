package com.heco.toolkit.hbase.export;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;

import com.hbase.api.BasicDDL;

public class ExportDDL {
	static final String NAME = "ExportDDL";
	
	static Connection connection;
	static Admin admin;
	static Logger logger = Logger.getLogger(ExportDDL.class);
	
	private static void connect(String zk,String port) {
		// HBaseConfiguration对象
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", zk);
		conf.set("hbase.zookeeper.property.clientPort", port);
		try {
			// Connection是操作HBase的入口
			connection = ConnectionFactory.createConnection(conf);
			admin = connection.getAdmin();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void close() throws IOException {
		if (admin != null)
			admin.close();
		if (connection != null)
			connection.close();
	}
	
	private static void usage(final String errorMsg) {
        if (errorMsg != null && errorMsg.length() > 0) {
            System.err.println("ERROR: " + errorMsg);
        }
        String usage = "Usage: " + NAME + " <hbase.zookeeper.quorum> <hbase.zookeeper.clientPort>\n\n"
        				+ "Backups HBase create statements in current path.\n\n"
        				+ "Pre-splitting default 'HexStringSplit', Choosing a appropriate one."; 
        System.err.println(usage);
    }
	
	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
            usage("Wrong number of arguments: " + args.length);
            System.exit(-1);
        }
		connect(args[0],args[1]);
		
		// 输出文件
		File file = new File("./HBaseTablesDDL.txt");
		OutputStreamWriter write = new OutputStreamWriter(new FileOutputStream(file));
		BufferedWriter bufferedWriter = new BufferedWriter(write);
		
		// 查询所有HBase的表
		TableName[] tbs = admin.listTableNames();
		Table table;
		for(TableName tb : tbs){
			table = connection.getTable(tb);
			StringBuffer sb = new StringBuffer();
			
			// 建表语句
			sb.append("create " + table.getTableDescriptor().toString().replace("TTL => 'FOREVER',", ""));
			// 分区信息
			List<HRegionInfo> regions = admin.getTableRegions(tb);
			if(regions.size() > 1){
				// 注意:输出时默认表选择的split算法是'HexStringSplit'（也可能是'UniformSplit'）
				sb.append(", {NUMREGIONS => " + regions.size() +  ", SPLITALGO => 'HexStringSplit'}");
			}
			
			bufferedWriter.write(sb.toString() + "\n");
		}
		
		bufferedWriter.close();
		close();
	}
	
	
	
}
