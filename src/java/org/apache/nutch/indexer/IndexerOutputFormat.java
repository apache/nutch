package org.apache.nutch.indexer;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

public class IndexerOutputFormat extends FileOutputFormat<Text, NutchDocument> {

  @Override
  public RecordWriter<Text, NutchDocument> getRecordWriter(FileSystem ignored,
      JobConf job, String name, Progressable progress) throws IOException {
    final NutchIndexWriter[] writers =
      NutchIndexWriterFactory.getNutchIndexWriters(job);

    for (final NutchIndexWriter writer : writers) {
      writer.open(job, name);
    }
    return new RecordWriter<Text, NutchDocument>() {

      public void close(Reporter reporter) throws IOException {
        for (final NutchIndexWriter writer : writers) {
          writer.close();
        }
      }

      public void write(Text key, NutchDocument doc) throws IOException {
        for (final NutchIndexWriter writer : writers) {
          writer.write(doc);
        }
      }
    };
  }
}
