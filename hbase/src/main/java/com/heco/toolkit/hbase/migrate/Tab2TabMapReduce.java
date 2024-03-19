package com.heco.toolkit.hbase.migrate;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Tab2TabMapReduce extends Configured implements Tool {

	private static Text rowkey = new Text();

	// mapper class
	public static class TabMapper extends TableMapper<Text, Put> {

		@Override
		protected void map(ImmutableBytesWritable key, Result value, Context context)
				throws IOException, InterruptedException {
			byte[] bytes = key.get();
			rowkey.set(Bytes.toString(bytes));
			System.out.println(rowkey.toString());
			Put put = new Put(bytes);
			for (Cell cell : value.rawCells()) {
				// add cell
				if ("f".equals(new String(CellUtil.cloneFamily(cell)))) {
					if ("q".equals(new String(CellUtil.cloneQualifier(cell)))) {
						//System.out.println(bytes.toString(cell.getRow()));
						put.add(cell);
						context.write(rowkey, put);
					}
				}
			}
		}
	}
	// reduce class
	public static class TabReduce extends TableReducer<Text, Put, ImmutableBytesWritable> {
		@Override
		protected void reduce(Text key, Iterable<Put> values, Context context)
				throws IOException, InterruptedException {
			for (Put put : values) {
				System.out.println("bytes"+Bytes.toString(put.getRow()));
				context.write(null, put);
			}
		}
	}
	public int run(String[] args) throws Exception {
		if(args.length !=2){
			System.err.println("ERROR: " + "Wrong number of arguments: " + args.length + "\n\n"
					+ "Usage: Tab2TabMapReduce <tab1> <tab2>");
			return -1;
		}
		String tab1 = args[0];
		String tab2 = args[1];
		
		// create job
		Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
		// set run class
		job.setJarByClass(this.getClass());
		// set scan
		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);
		// set mapper
		TableMapReduceUtil.initTableMapperJob(tab1, scan, TabMapper.class, Text.class, Put.class, job);
		// set reduce
		TableMapReduceUtil.initTableReducerJob(tab2, TabReduce.class, job);
		job.setNumReduceTasks(1);

		boolean b = job.waitForCompletion(true);
		if(!b) {
			System.err.print("error with job!!!");
		}
		return 0;
	}
	public static void main(String[] args) throws Exception {
		// create conf
		Configuration config = HBaseConfiguration.create();
		// submit job
		int status = ToolRunner.run(config, new Tab2TabMapReduce(), args);
		// exit
		System.exit(status);
	}
}
