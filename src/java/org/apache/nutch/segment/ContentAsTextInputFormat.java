package org.apache.nutch.segment;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
import org.apache.nutch.protocol.Content;

/**
 * An input format that takes Nutch Content objects and converts them to text
 * while converting newline endings to spaces.  This format is useful for working
 * with Nutch content objects in Hadoop Streaming with other languages.
 */
public class ContentAsTextInputFormat
  extends SequenceFileInputFormat<Text, Text> {

  private static class ContentAsTextRecordReader
    implements RecordReader<Text, Text> {

    private final SequenceFileRecordReader<Text, Content> sequenceFileRecordReader;

    private Text innerKey;
    private Content innerValue;

    public ContentAsTextRecordReader(Configuration conf, FileSplit split)
      throws IOException {
      sequenceFileRecordReader = new SequenceFileRecordReader<Text, Content>(
        conf, split);
      innerKey = (Text)sequenceFileRecordReader.createKey();
      innerValue = (Content)sequenceFileRecordReader.createValue();
    }

    public Text createKey() {
      return new Text();
    }

    public Text createValue() {
      return new Text();
    }

    public synchronized boolean next(Text key, Text value)
      throws IOException {
      
      // convert the content object to text
      Text tKey = key;
      Text tValue = value;
      if (!sequenceFileRecordReader.next(innerKey, innerValue)) {
        return false;
      }
      tKey.set(innerKey.toString());
      String contentAsStr = new String(innerValue.getContent());
      
      // replace new line endings with spaces
      contentAsStr = contentAsStr.replaceAll("\n", " ");
      value.set(contentAsStr);
     
      return true;
    }

    public float getProgress()
      throws IOException {
      return sequenceFileRecordReader.getProgress();
    }

    public synchronized long getPos()
      throws IOException {
      return sequenceFileRecordReader.getPos();
    }

    public synchronized void close()
      throws IOException {
      sequenceFileRecordReader.close();
    }
  }

  public ContentAsTextInputFormat() {
    super();
  }

  public RecordReader<Text, Text> getRecordReader(InputSplit split,
    JobConf job, Reporter reporter)
    throws IOException {

    reporter.setStatus(split.toString());
    return new ContentAsTextRecordReader(job, (FileSplit)split);
  }
}
