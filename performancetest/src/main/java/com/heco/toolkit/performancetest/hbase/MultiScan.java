//package com.heco.toolkit.performancetest.hbase;
//
//import java.io.IOException;
//import java.util.LinkedList;
//import java.util.List;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.client.*;
//import org.apache.hadoop.hbase.util.Bytes;
//
//import java.io.BufferedReader;
//import java.io.FileNotFoundException;
//import java.io.FileReader;
//import java.util.Date;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.text.DateFormat;
//import java.text.ParseException;
//import java.text.SimpleDateFormat;
//
//public class MultiScan {
//
//	public static void main(String[] args) {
//
//	    if(args.length != 4){
//	    	System.out.println("input: [tableName] [conditionsFilePath] [threadNumber] [outputInterval]");
//	    	System.exit(2);
//	    }
//	    String tableName = args[0];
//	    String condFile = args[1];
//	    int threadNum = Integer.parseInt(args[2]);
//	    int outputInterval = Integer.parseInt(args[3]);
//	    System.out.println("----conditionsFilePath:\t" + condFile);
//	    System.out.println("----threadNumber:\t" + threadNum);
//
//	    List<String>[] conditions = new List[threadNum];
////	    System.out.println("list.size: " + conditions.length);
//	    BufferedReader br = null;
//		try {
//			br = new BufferedReader(new FileReader(condFile));
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	    String line = "";
//	    for(int i = 0; i < conditions.length; i++){
//	    	conditions[i] = new LinkedList<String>();
//	    }
//	    try {
//			for(int i = 0; (line = br.readLine()) != null; i++) {
//				conditions[i%threadNum].add(line);
//			}
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
////
////	    for(int i = 0; i < conditions.length; i++){
////	    	System.out.println("conditions[" + i + "]");
////	    	for(String str : conditions[i]){
////	    		System.out.println(str+"----");
////	    	}
////	    }
////	    System.exit(2);
//
//	    long startTime = System.currentTimeMillis();
//
//	    ExecutorService threadpool = Executors.newFixedThreadPool(threadNum);
//	    for(int i = 0; i < threadNum; i++) {
//	    	ExecuteScan scan = new ExecuteScan(conditions[i], tableName, outputInterval);
//	    	threadpool.execute(scan);
//	    }
//    	threadpool.shutdown();
//    	while(!threadpool.isTerminated()){
//    		try {
//				Thread.sleep(1000);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//    	}
//    	long endTime = System.currentTimeMillis();
//    	System.out.println("----Scan over!");
//    	System.out.println("----Total used time: " + (endTime - startTime) + " ms.");
//    	Configuration conf = HBaseConfiguration.create();
//    	try {
//			Table table = new HTable(conf, tableName);
//			table.close();
//			System.out.println("----close HBase table: " + tableName);
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
//}
//
//class ExecuteScan implements Runnable {
//	private Configuration conf = HBaseConfiguration.create();
//	private List<String> conditions = null;
//	private String tableName = "";
//	private int outputInterval = 10;
//	private String condition = "";
//	private DateFormat df = new SimpleDateFormat("yyyyMMdd");
//	private int scanTime = 0;
//	private int resNum = 0;
//	public ExecuteScan(List<String> conditions, String tableName, int outputInterval){
//		this.conditions = conditions;
//		this.tableName = tableName;
//		this.outputInterval = outputInterval;
//	}
//
//	@Override
//	public void run() {
//		// TODO Auto-generated method stub
//
//		HTable table = null;
//		try {
//			table = new HTable(conf, tableName);
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		int condLen = conditions.size();
//		for (int i = 0; i < condLen; i++){
////			System.out.println("-------" + condition + "--------");
//			condition = conditions.get(i);
////			String prefix = String.format("%0" + 4 + "d", (condition.hashCode()&0xffff)%10000);
////			String startRow = prefix + condition;
//			String prefix = new StringBuilder(condition.substring(0, 11)).reverse().toString();
//			String startTime = condition.substring(11, 19);
//			String suffix = condition.substring(19);
//			String startRow = prefix + startTime + suffix;
//
////			String str = condition.substring(0,19);
////			String endTime = condition.substring(19);
//			Date endDate = null;
//			try {
//				endDate = df.parse(suffix);
//			} catch (ParseException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//			long longtime= endDate.getTime()+3600*24*1000;
//			Date date2 = new Date(longtime);
//			String endTime = df.format(date2);
//			String stopRow = prefix + startTime + endTime;
//
//
//			Scan scan = new Scan();
//			scan.setStartRow(Bytes.toBytes(startRow));
//			scan.setStopRow(Bytes.toBytes(stopRow));
//			ResultScanner rs = null;
//			try {
//				rs = table.getScanner(scan);
//				scanTime++;
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
////			System.out.println("startRow: " + startRow + " stopRow: " + stopRow );
//			if(rs != null) {
////				resNum++;
//				System.out.print(".");
//				for (Result r : rs) {
////	                System.out.println("获得到rowkey:" + new String(r.getRow()));
////	                for (KeyValue keyValue : r.raw()) {
////	                    System.out.println("列：" + new String(keyValue.getFamily())
////	                            + "====值:" + new String(keyValue.getValue()));
////	                }
//					resNum++;
//					if(resNum%outputInterval == 0){
//						System.out.println("\nstartRow: " + startRow + " stopRow: " + stopRow + " --RowKey: " + new String(r.getRow()) + "  --Value: " + new String(r.getValue("c".getBytes(), "a".getBytes())));
//					}
//	            }
////				if(resNum%outputInterval == 0){
//////					System.out.println("startRow: " + startRow + " stopRow: " + stopRow);
////					Iterator<Result> res = rs.iterator();
////					System.out.print("="+res.hasNext());
//////					for (Result r : rs) {
////					while(res.hasNext()) {
////						Result r = res.next();
////						System.out.print("+");
////						System.out.println("startRow: " + startRow + " stopRow: " + stopRow + " --RowKey: " + new String(r.getRow()) + "  --Value: " + new String(r.getValue("c".getBytes(), "a".getBytes())));
//////						for (Cell cell : r.rawCells()) {
//////							System.out.print("--RowKey: " + new String(CellUtil.cloneRow(cell)) + "  --Value: " + new String(CellUtil.cloneValue(cell)));
//////						System.out.print("----RowKry: " + new String(CellUtil.cloneRow(cell)));
//////						System.out.print("  --ColumnFamily: " + new String(CellUtil.cloneFamily(cell)));
//////						System.out.print("  --Column: " + new String(CellUtil.cloneQualifier(cell)));
//////						System.out.print("  --Value: " + new String(CellUtil.cloneValue(cell)));
//////						System.out.println("  --Timestamp: " + cell.getTimestamp() + "--");
//////						}
////					}
////				}
//			} else {
//				System.out.print("-");
//			}
////			condition = ConditionEntity.getCondition();
//		}
//		System.out.println("====scan times: " + scanTime + " ====resault number: "+ resNum);
//
//
//	}
//
//}
//
