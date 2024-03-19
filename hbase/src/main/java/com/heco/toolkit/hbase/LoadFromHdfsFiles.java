package com.heco.toolkit.hbase;

import java.io.IOException;

public class LoadFromHdfsFiles {
	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		if(args.length != 1){
			System.out.println("Please input the path on HDFS. Like /temp");
			System.exit(0);
		}
		int fileNum = Integer.parseInt(args[0]);
//		String pathOnHdfs = args[0];
//		HDFSUtilCKT hdfsUtil = new HDFSUtilCKT();
//		List<String> filesInPath = hdfsUtil.listAllFileInPath(pathOnHdfs);
//		Configuration conf = new Configuration();
//		FileSystem fs = FileSystem.get(conf);
		String path = "/temp/hbasetest/hbasetestdata";
		long startTime = System.currentTimeMillis();
		for(int i=0;i<fileNum;i++){
//			Path path = new Path(file);
//			if(fs.isFile(path)){
			String  file = path+i+".txt";
				System.out.println("file is: " + file);
				PutHdfsFileToHbase fileToHbase = new PutHdfsFileToHbase(file);
				 Thread thread = new Thread(fileToHbase);
				thread.start();
				System.out.println("Strat thread of: " + file);
//			}
		}
		long endTime = System.currentTimeMillis();
		System.out.println("LoadFromHdfsFiles used time:" + (endTime-startTime));
//		fs.close();
	}

}
