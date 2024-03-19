package com.heco.toolkit.hbase.importtsv;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.CellCreator;
import org.apache.hadoop.hbase.mapreduce.ImportTsv.TsvParser.BadTsvLineException;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.apache.hadoop.hbase.security.visibility.InvalidLabelException;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Write table content out to files in hdfs.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class MyTsvImporterMapper
extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>
{

  /** Timestamp for all inserted rows */
  protected long ts;

  /** Column seperator */
  private String separator;

  /** Should skip bad lines */
  private boolean skipBadLines;
  private Counter badLineCount;

  protected MyImportTsv.TsvParser parser;

  protected Configuration conf;

  protected String cellVisibilityExpr;

  protected long ttl;

  protected CellCreator kvCreator;

  private String hfileOutPath;

  public long getTs() {
    return ts;
  }

  public boolean getSkipBadLines() {
    return skipBadLines;
  }

  public Counter getBadLineCount() {
    return badLineCount;
  }

  public void incrementBadLineCount(int count) {
    this.badLineCount.increment(count);
  }

  /**
   * Handles initializing this class with objects specific to it (i.e., the parser).
   * Common initialization that might be leveraged by a subsclass is done in
   * <code>doSetup</code>. Hence a subclass may choose to override this method
   * and call <code>doSetup</code> as well before handling it's own custom params.
   *
   * @param context
   */
  @Override
  protected void setup(Context context) {
    doSetup(context);

    conf = context.getConfiguration();
    parser = new MyImportTsv.TsvParser(conf.get(MyImportTsv.COLUMNS_CONF_KEY),
                           separator);
    if (parser.getRowKeyColumnIndex() == -1) {
      throw new RuntimeException("No row key column specified");
    }
    this.kvCreator = new CellCreator(conf);
  }

  /**
   * Handles common parameter initialization that a subclass might want to leverage.
   * @param context
   */
  protected void doSetup(Context context) {
    Configuration conf = context.getConfiguration();

    // If a custom separator has been used,
    // decode it back from Base64 encoding.
    separator = conf.get(MyImportTsv.SEPARATOR_CONF_KEY);
    if (separator == null) {
      separator = MyImportTsv.DEFAULT_SEPARATOR;
    } else {
      separator = new String(Base64.decode(separator));
    }
    // Should never get 0 as we are setting this to a valid value in job
    // configuration.
    ts = conf.getLong(MyImportTsv.TIMESTAMP_CONF_KEY, 0);

    skipBadLines = context.getConfiguration().getBoolean(
        MyImportTsv.SKIP_LINES_CONF_KEY, true);
    badLineCount = context.getCounter("MyImportTsv", "Bad Lines");
    hfileOutPath = conf.get(MyImportTsv.BULK_OUTPUT_CONF_KEY);
  }

//  //java 合并两个byte数组
//	public static byte[] byteMerger(byte[] byte_1, byte[] byte_2) {
//		byte[] byte_3 = new byte[byte_1.length + byte_2.length];
//		System.arraycopy(byte_1, 0, byte_3, 0, byte_1.length);
//		System.arraycopy(byte_2, 0, byte_3, byte_1.length, byte_2.length);
//		return byte_3;
//	}
	
  /**
   * Convert a line of TSV text into an HBase table row.
   */
  @Override
  public void map(LongWritable offset, Text value,
    Context context)
  throws IOException {
	
//	  // lyj
//	  String[] words = value.toString().split("|");
//	  String[] items = words[0].split("\u0001");
//	  
//	  byte[] row = Bytes.toBytes(items[0].hashCode() & 0x7fff);
//		for(String item : items){
//			row = Bytes.add(row, Bytes.toBytes("\u0001"),Bytes.toBytes(item));
//		}
//	  byte[] qvalue = Bytes.toBytes(words[1]);
//	  
//	  byte[] lineBytesNew = byteMerger(row,qvalue);
//	  Text valueNew = new Text();
//	  valueNew.set(lineBytesNew);
	  
    byte[] lineBytes = value.getBytes();

    try {
      MyImportTsv.TsvParser.ParsedLine parsed = parser.parse(
    		  lineBytes, value.getLength());
      ImmutableBytesWritable rowKey =
        new ImmutableBytesWritable(lineBytes,
            parsed.getRowKeyOffset(),
            parsed.getRowKeyLength());
      
      // Retrieve timestamp if exists
      ts = parsed.getTimestamp(ts);
      cellVisibilityExpr = parsed.getCellVisibility();
      ttl = parsed.getCellTTL();

      Put put = new Put(rowKey.copyBytes());
      for (int i = 0; i < parsed.getColumnCount(); i++) {
        if (i == parser.getRowKeyColumnIndex() || i == parser.getTimestampKeyColumnIndex()
            || i == parser.getAttributesKeyColumnIndex() || i == parser.getCellVisibilityColumnIndex()
            || i == parser.getCellTTLColumnIndex()) {
          continue;
        }
        populatePut(lineBytes, parsed, put, i);
      }
      context.write(rowKey, put);
    } catch (InvalidLabelException badLine) {
      if (skipBadLines) {
        System.err.println(
            "Bad line at offset: " + offset.get() + ":\n" +
            badLine.getMessage());
        incrementBadLineCount(1);
        return;
      } else {
        throw new IOException(badLine);
      }
    } catch (MyImportTsv.TsvParser.BadTsvLineException badLine) {
      if (skipBadLines) {
        System.err.println(
            "Bad line at offset: " + offset.get() + ":\n" +
            badLine.getMessage());
        incrementBadLineCount(1);
        return;
      } else {
        throw new IOException(badLine);
      }
    } catch (IllegalArgumentException e) {
      if (skipBadLines) {
        System.err.println(
            "Bad line at offset: " + offset.get() + ":\n" +
            e.getMessage());
        incrementBadLineCount(1);
        return;
      } else {
        throw new IOException(e);
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (BadTsvLineException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }

  protected void populatePut(byte[] lineBytes, MyImportTsv.TsvParser.ParsedLine parsed, Put put,
      int i) throws BadTsvLineException, IOException {
    Cell cell = null;
    if (hfileOutPath == null) {
      cell = new KeyValue(lineBytes, parsed.getRowKeyOffset(), parsed.getRowKeyLength(),
          parser.getFamily(i), 0, parser.getFamily(i).length, parser.getQualifier(i), 0,
          parser.getQualifier(i).length, ts, KeyValue.Type.Put, lineBytes,
          parsed.getColumnOffset(i), parsed.getColumnLength(i));
      if (cellVisibilityExpr != null) {
        // We won't be validating the expression here. The Visibility CP will do
        // the validation
        put.setCellVisibility(new CellVisibility(cellVisibilityExpr));
      }
      if (ttl > 0) {
        put.setTTL(ttl);
      }
    } else {
      // Creating the KV which needs to be directly written to HFiles. Using the Facade
      // KVCreator for creation of kvs.
      List<Tag> tags = new ArrayList<Tag>();
      if (cellVisibilityExpr != null) {
        tags.addAll(kvCreator.getVisibilityExpressionResolver()
          .createVisibilityExpTags(cellVisibilityExpr));
      }
      // Add TTL directly to the KV so we can vary them when packing more than one KV
      // into puts
      if (ttl > 0) {
        tags.add(new Tag(TagType.TTL_TAG_TYPE, Bytes.toBytes(ttl)));
      }
      cell = this.kvCreator.create(lineBytes, parsed.getRowKeyOffset(), parsed.getRowKeyLength(),
          parser.getFamily(i), 0, parser.getFamily(i).length, parser.getQualifier(i), 0,
          parser.getQualifier(i).length, ts, lineBytes, parsed.getColumnOffset(i),
          parsed.getColumnLength(i), tags);
    }
    put.add(cell);
  }
}
