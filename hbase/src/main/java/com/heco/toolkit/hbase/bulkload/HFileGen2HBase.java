package com.heco.toolkit.hbase.bulkload;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/**
 * HFile生成 并bulkload到表(列族f列q)
 * 输入文件rowkey与value以\u0002分隔、待处理rowkey以\u0001分隔
 */
public class HFileGen2HBase extends Configured implements Tool {

	final static String NAME = "HFileGen2HBase";
	
	public static class HFileGen2HbaseMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>{
		private static final byte[] FAMILY_BYTE = Bytes.toBytes("f");
		private static final byte[] QUALIFIER_INDEX = Bytes.toBytes("q");

		@SuppressWarnings("deprecation")
		@Override
		protected void map(LongWritable offset, Text value, Context context)
				throws IOException, InterruptedException {
			// 得到rowKey以及value，并转成字节
			
			// line
			String line = value.toString();
			String[] words = line.split("\u0002");
			
			// 取rowkey中主键值hash后作前缀
			String[] items = words[0].split("\u0001");
//			if(items != null && items.length > 1){
				byte[] row = Bytes.toBytes(items[0].hashCode() & 0x7fff);
				for(String item : items){
					row = Bytes.add(row, Bytes.toBytes("\u0001"),Bytes.toBytes(item));
				}
				//byte[] rowkey = Bytes.toBytes(words[0]);
				byte[] qvalue = Bytes.toBytes("");
				if(words.length > 1)
				{
					qvalue = Bytes.toBytes(words[1]);
				}

				// 生成Put对象
				ImmutableBytesWritable rowkey = new ImmutableBytesWritable(row);
				Put put = new Put(rowkey.copyBytes());
				put.add(FAMILY_BYTE, QUALIFIER_INDEX, qvalue);

				// 输出
				context.write(rowkey, put);
//			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		if(args.length !=3){
			System.err.println("ERROR: " + "Wrong number of arguments: " + args.length + "\n\n"
					+ "Usage: " + NAME + " <inputPath> <outputPath> <tableName>");
			return -1;
		}
		
		//Configuration conf = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(getConf());

		// create job
		Job job = Job.getInstance(getConf(), "HFileBulkload_" + args[2]);
		
		// set run jar class
		job.setJarByClass(HFileGen2HBase.class);

		// set map
		job.setMapperClass(HFileGen2HbaseMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);
		
		// set reduce //可不指定 框架会根据MapOutputValueClass来决定是使用 KeyValueSortReducer还是PutSortReducer
		job.setReducerClass(PutSortReducer.class);

		// set input and output path
		//String inputPath = "hdfs://beh/test/data.txt"; // 可指定路径
		//String outputPath = "hdfs://beh/test/hfiles";  // 目录不能提前存在
		String inputPath = args[0];
		String outputPath = args[1];
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		
		HTable table = (HTable) connection.getTable(TableName.valueOf(args[2]));
		
		// set hfile output
		HFileOutputFormat2.configureIncrementalLoad(job,table);

		// submit job
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!!!");
		}
		
		// bulk load
		LoadIncrementalHFiles loadFiles = new LoadIncrementalHFiles(getConf());
		loadFiles.doBulkLoad(new Path(outputPath),table);
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration config = HBaseConfiguration.create();
		// submit job
		int status = ToolRunner.run(config, new HFileGen2HBase(), args);
		// exit
		System.exit(status);
	}
	
}
