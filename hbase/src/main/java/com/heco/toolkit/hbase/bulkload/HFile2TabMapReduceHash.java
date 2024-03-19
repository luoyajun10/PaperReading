package com.heco.toolkit.hbase.bulkload;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
// zhangyulong
public class HFile2TabMapReduceHash extends Configured implements Tool {

	public static class HFile2TabMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

		ImmutableBytesWritable rowkey = new ImmutableBytesWritable();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] words = value.toString().split("\u0002");

			for (String word:words){
				System.out.println("word:"+word);
			}
			
			byte[] rowKeynew =gethashrowkey(convertStrToArray2(words[0],"\u0001"));
			
			Put put = new Put(rowKeynew);
			put.add(Bytes.toBytes("f"), Bytes.toBytes("q"), Bytes.toBytes(words[1]));
			rowkey.set(rowKeynew);

			context.write(rowkey, put);
		}
	}

	public static byte[] gethashrowkey(String[] strArray) {
		byte[] rowkey = Bytes.toBytes(strArray[0].hashCode() & 0x7fff);
		for (String str : strArray) {
			rowkey = Bytes.add(rowkey, Bytes.toBytes("\u0001"), Bytes.toBytes(str));
		}
		return rowkey;
	}

	public static String[] convertStrToArray2(String str, String separatorStr) {
		StringTokenizer st = new StringTokenizer(str, separatorStr);
		String[] strArray = new String[st.countTokens()];
		int i = 0;
		while (st.hasMoreTokens()) {
			strArray[i++] = st.nextToken();
		}
		return strArray;
	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		// create job
		Job job = Job.getInstance(getConf(), this.getClass().getSimpleName());

		// set run jar class
		job.setJarByClass(this.getClass());

		System.out.println("args[0]:" + args[0]);
		// set input output
		FileInputFormat.addInputPath(job, new Path(args[1]));
		System.out.println("args[1]:" + args[1]);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.out.println("args[2]:" + args[2]);

		// set map
		job.setMapperClass(HFile2TabMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);

		// set reduce
		job.setReducerClass(PutSortReducer.class);

		HTable table = new HTable(getConf(), args[0]);
		// set hfile output
		HFileOutputFormat2.configureIncrementalLoad(job, table);

		// submit job
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!!!");
		}
		LoadIncrementalHFiles loader = new LoadIncrementalHFiles(getConf());
		loader.doBulkLoad(new Path(args[2]), table);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration config = HBaseConfiguration.create();
		// config.addResource(new
		// Path("/opt/beh/core/hbase/conf/hbase-site.xml"));
		config.set("hbase.rootdir", "hdfs://breath:9000/hbase");
		// submit job
		int status = ToolRunner.run(config, new HFile2TabMapReduceHash(), args);
		// exit
		System.exit(status);

	}

}
