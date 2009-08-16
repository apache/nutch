package org.apache.nutch.util.hbase;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.conf.*;

import org.apache.nutch.util.NutchConfiguration;

public class WebTableCreator extends Configured implements Tool {

  public static final Log LOG = LogFactory.getLog(WebTableCreator.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(),
        new WebTableCreator(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {

    if (args.length != 1) {
      System.err.println("Usage: WebTableCreator <webtable>");
      return -1;
    }

    try {
      HBaseConfiguration hbaseConf = new HBaseConfiguration();

      LOG.info("WebTableCreator: Creating table: " + args[0]);
      HTableDescriptor tableDesc = new HTableDescriptor(args[0]);

      final Field[] fields = WebTableColumns.class.getFields();

      for (final Field field : fields) {
        final Annotation ann = field.getAnnotation(ColumnDescriptor.class);
        if (ann == null) {
          continue;
        }
        ColumnDescriptor colDesc = (ColumnDescriptor) ann;
        HColumnDescriptor family = new HColumnDescriptor((byte[])field.get(null),
            colDesc.versions(), colDesc.compression().getName(),
            colDesc.inMemory(), colDesc.blockCacheEnabled(), 
            colDesc.blockSize(), colDesc.timeToLive(), colDesc.bloomFilter());
        tableDesc.addFamily(family);
      }

      HBaseAdmin admin = new HBaseAdmin(hbaseConf);
      admin.createTable(tableDesc);
      LOG.info("WebTableCreator: Done.");
      return 0;
    } catch (Exception e) {
      LOG.fatal("WebTableCreator: " + StringUtils.stringifyException(e));
      return -1;
    }

  }
}
