/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.indexer.field;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.FSDirectory;
import org.apache.nutch.analysis.AnalyzerFactory;
import org.apache.nutch.analysis.NutchAnalyzer;
import org.apache.nutch.analysis.NutchDocumentAnalyzer;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.NutchSimilarity;
import org.apache.nutch.util.LogUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;

public class FieldIndexer
  extends Configured
  implements Tool, Mapper<Text, Writable, Text, FieldWritable>,
  Reducer<Text, FieldWritable, Text, FieldIndexer.LuceneDocumentWrapper> {

  public static final Log LOG = LogFactory.getLog(FieldIndexer.class);
  public static final String DONE_NAME = "index.done";

  private FieldFilters fieldFilters;

  public static class LuceneDocumentWrapper
    implements Writable {
    private Document doc;

    public LuceneDocumentWrapper(Document doc) {
      this.doc = doc;
    }

    public Document get() {
      return doc;
    }

    public void readFields(DataInput in)
      throws IOException {
      // intentionally left blank
    }

    public void write(DataOutput out)
      throws IOException {
      // intentionally left blank
    }

  }

  public static class OutputFormat
    extends FileOutputFormat<WritableComparable, LuceneDocumentWrapper> {

    public RecordWriter<WritableComparable, LuceneDocumentWrapper> getRecordWriter(
      final FileSystem fs, JobConf job, String name, final Progressable progress)
      throws IOException {

      final Path perm = new Path(FileOutputFormat.getOutputPath(job), name);
      final Path temp = job.getLocalPath("index/_"
        + Integer.toString(new Random().nextInt()));

      fs.delete(perm, true); // delete old, if any

      final AnalyzerFactory factory = new AnalyzerFactory(job);
      final IndexWriter writer = // build locally first
      new IndexWriter(
        FSDirectory.open(new File(fs.startLocalOutput(perm, temp).toString())),
        new NutchDocumentAnalyzer(job), true, 
        new MaxFieldLength(IndexWriter.DEFAULT_MAX_FIELD_LENGTH));

      writer.setMergeFactor(job.getInt("indexer.mergeFactor", 10));
      writer.setMaxBufferedDocs(job.getInt("indexer.minMergeDocs", 100));
      writer.setMaxMergeDocs(job.getInt("indexer.maxMergeDocs",
        Integer.MAX_VALUE));
      writer.setTermIndexInterval(job.getInt("indexer.termIndexInterval", 128));
      writer.setMaxFieldLength(job.getInt("indexer.max.tokens", 10000));
      writer.setInfoStream(LogUtil.getInfoStream(LOG));
      writer.setUseCompoundFile(false);
      writer.setSimilarity(new NutchSimilarity());

      return new RecordWriter<WritableComparable, LuceneDocumentWrapper>() {
        boolean closed;

        public void write(WritableComparable key, LuceneDocumentWrapper value)
          throws IOException { // unwrap & index doc
          Document doc = value.get();
          NutchAnalyzer analyzer = factory.get(doc.get("lang"));
          if (LOG.isInfoEnabled()) {
            LOG.info(" Indexing [" + doc.getField("url").stringValue() + "]"
              + " with analyzer " + analyzer);
          }
          writer.addDocument(doc, analyzer);
          progress.progress();
        }

        public void close(final Reporter reporter)
          throws IOException {

          // spawn a thread to give progress heartbeats
          Thread prog = new Thread() {
            public void run() {
              while (!closed) {
                try {
                  reporter.setStatus("closing");
                  Thread.sleep(1000);
                }
                catch (InterruptedException e) {
                  continue;
                }
                catch (Throwable e) {
                  return;
                }
              }
            }
          };

          try {
            prog.start();
            if (LOG.isInfoEnabled()) {
              LOG.info("Optimizing index.");
            }
            // optimize & close index
            writer.optimize();
            writer.close();
            fs.completeLocalOutput(perm, temp); // copy to dfs
            fs.createNewFile(new Path(perm, DONE_NAME));
          }
          finally {
            closed = true;
          }
        }
      };
    }
  }

  public FieldIndexer() {

  }

  public FieldIndexer(Configuration conf) {
    setConf(conf);
  }

  public void configure(JobConf job) {
    setConf(job);
    this.fieldFilters = new FieldFilters(job);
  }

  public void close() {
  }

  public void map(Text key, Writable value,
    OutputCollector<Text, FieldWritable> output, Reporter reporter)
    throws IOException {

    if (value instanceof FieldsWritable) {
      FieldsWritable fields = (FieldsWritable)value;
      List<FieldWritable> fieldsList = fields.getFieldsList();
      for (FieldWritable field : fieldsList) {
        output.collect(key, field);
      }
    }
    else if (value instanceof FieldWritable) {
      output.collect(key, (FieldWritable)value);
    }
  }

  public void reduce(Text key, Iterator<FieldWritable> values,
    OutputCollector<Text, LuceneDocumentWrapper> output, Reporter reporter)
    throws IOException {

    Document doc = new Document();
    List<FieldWritable> fieldsList = new ArrayList<FieldWritable>();
    Configuration conf = getConf();

    while (values.hasNext()) {
      FieldWritable field = values.next();
      fieldsList.add((FieldWritable)WritableUtils.clone(field, conf));
    }

    try {
      doc = fieldFilters.filter(key.toString(), doc, fieldsList);
    }
    catch (IndexingException e) {
      throw new IOException(e);
    }
    
    if (doc != null) {
      output.collect(key, new LuceneDocumentWrapper(doc));
    }
  }

  public void index(Path[] fields, Path indexDir)
    throws IOException {

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.info("FieldIndexer: starting at " + sdf.format(start));

    JobConf job = new NutchJob(getConf());
    job.setJobName("FieldIndexer: " + indexDir);

    for (int i = 0; i < fields.length; i++) {
      Path fieldsDb = fields[i];
      LOG.info("FieldIndexer: adding fields db: " + fieldsDb);
      FileInputFormat.addInputPath(job, fieldsDb);
    }

    job.setInputFormat(SequenceFileInputFormat.class);
    job.setMapperClass(FieldIndexer.class);
    job.setReducerClass(FieldIndexer.class);
    FileOutputFormat.setOutputPath(job, indexDir);
    job.setOutputFormat(OutputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(FieldWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LuceneDocumentWrapper.class);

    JobClient.runJob(job);
    long end = System.currentTimeMillis();
    LOG.info("FieldIndexer: finished at " + sdf.format(end) + ", elapsed: " + TimingUtil.elapsedTime(start, end));
  }

  public static void main(String[] args)
    throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new FieldIndexer(),
      args);
    System.exit(res);
  }

  public int run(String[] args)
    throws Exception {

    Options options = new Options();
    Option helpOpts = OptionBuilder.withArgName("help").withDescription(
      "show this help message").create("help");
    Option outputOpts = OptionBuilder.withArgName("output").hasArg().withDescription(
      "the output index directory").create("output");
    Option fieldOpts = OptionBuilder.withArgName("fields").hasArgs().withDescription(
      "the field database(s) to use").create("fields");
    options.addOption(helpOpts);
    options.addOption(fieldOpts);
    options.addOption(outputOpts);

    CommandLineParser parser = new GnuParser();
    try {

      CommandLine line = parser.parse(options, args);
      if (line.hasOption("help") || !line.hasOption("fields")
        || !line.hasOption("output")) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("FieldIndexer", options);
        return -1;
      }

      Path output = new Path(line.getOptionValue("output"));
      String[] fields = line.getOptionValues("fields");
      Path[] fieldPaths = new Path[fields.length];
      for (int i = 0; i < fields.length; i++) {
        fieldPaths[i] = new Path(fields[i]);
      }

      index(fieldPaths, output);
      return 0;
    }
    catch (Exception e) {
      LOG.fatal("FieldIndexer: " + StringUtils.stringifyException(e));
      return -2;
    }
  }
}
