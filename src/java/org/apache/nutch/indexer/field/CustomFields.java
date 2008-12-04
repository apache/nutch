package org.apache.nutch.indexer.field;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;

/**
 * Creates custom FieldWritable objects from a text file containing field
 * information including field name, value, and optional boost and fields type
 * (as needed by FieldWritable objects).
 * 
 * An input text file to CustomFields would be tab separated and would look
 * similar to this:
 * 
 * <pre> 
 * http://www.apache.org\tlang\ten\t5.0\tCONTENT
 * http://lucene.apache.org\tlang\tde
 * </pre>
 * 
 * The only required fields are url, name and value. Custom fields are
 * configured through the custom-fields.xml file in the classpath. The config
 * file allow you to set defaults for whether a field is indexed, stored, and
 * tokenized, boosts on a field, and whether a field can output multiple values
 * under the same key.
 * 
 * The purpose of the CustomFields job is to allow better integration with
 * technologies such as Hadoop Streaming. Streaming jobs can be created in any
 * programming language, can output the text file needed by the CustomFields
 * job, and those fields can then be included in the index.
 * 
 * The concept of custom fields requires two separate pieces. The indexing piece
 * and the query piece. The indexing piece is handled by the CustomFields job.
 * The query piece is handled by the query-custom plugin.
 * 
 * <b>Important:</b><br> <i>Currently, because of the way the query plugin
 * architecture works, custom fields names must be added to the fields parameter
 * in the query-custom plugin plugin.xml file in order to be queried.</i>
 * 
 * The CustomFields tool accepts one or more directories containing text files
 * in the appropriate custom field format. These files are then turned into
 * FieldWritable objects to be included in the index.
 */
public class CustomFields
  extends Configured
  implements Tool {

  public static final Log LOG = LogFactory.getLog(CustomFields.class);

  /**
   * MapReduce job that converts text values into FieldWritable objects.
   * 
   * @param inputs The directories with text files to convert.
   * @param output The converter output directory.
   * 
   * @throws IOException If an error occurs while converting.
   */
  private void runConverter(Path[] inputs, Path output)
    throws IOException {

    JobConf converter = new NutchJob(getConf());
    converter.setJobName("CustomFields Converter");
    for (int i = 0; i < inputs.length; i++) {
      FileInputFormat.addInputPath(converter, inputs[i]);
    }
    FileOutputFormat.setOutputPath(converter, output);
    converter.setInputFormat(TextInputFormat.class);
    converter.setMapperClass(Converter.class);
    converter.setReducerClass(Converter.class);
    converter.setMapOutputKeyClass(Text.class);
    converter.setMapOutputValueClass(FieldWritable.class);
    converter.setOutputKeyClass(Text.class);
    converter.setOutputValueClass(FieldWritable.class);
    converter.setOutputFormat(SequenceFileOutputFormat.class);

    LOG.info("Starting converter job");
    try {
      JobClient.runJob(converter);
    }
    catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw e;
    }
    LOG.info("Finished converter job.");
  }

  /**
   * Aggregated multiple FieldWritable objects with the same name. Depending on
   * settings in the custom-fields.xml file, a field may one or more fields.
   * This jobs aggregates fields and then collects based on the configuration
   * settings.
   * 
   * @param basicFields The basicfields FieldWritable objects.
   * @param converted The converted custom field objects.
   * @param output The final output directory for custom field objects.
   * 
   * @throws IOException If an error occurs while converting.
   */
  private void runCollector(Path basicFields, Path converted, Path output)
    throws IOException {

    JobConf collector = new NutchJob(getConf());
    collector.setJobName("CustomFields Collector");
    FileInputFormat.addInputPath(collector, converted);
    FileInputFormat.addInputPath(collector, basicFields);
    FileOutputFormat.setOutputPath(collector, output);
    collector.setInputFormat(SequenceFileInputFormat.class);
    collector.setMapOutputKeyClass(Text.class);
    collector.setMapOutputValueClass(ObjectWritable.class);
    collector.setMapperClass(Collector.class);
    collector.setReducerClass(Collector.class);
    collector.setOutputKeyClass(Text.class);
    collector.setOutputValueClass(FieldWritable.class);
    collector.setOutputFormat(SequenceFileOutputFormat.class);

    LOG.info("Starting collector job");
    try {
      JobClient.runJob(collector);
    }
    catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw e;
    }
    LOG.info("Finished collector job.");
  }

  /**
   * Converts text values into FieldWritable objects.
   */
  public static class Converter
    extends Configured
    implements Mapper<LongWritable, Text, Text, FieldWritable>,
    Reducer<Text, FieldWritable, Text, FieldWritable> {

    private JobConf conf;
    private Map<String, boolean[]> flagMap = new HashMap<String, boolean[]>();
    private Set<String> multiFields = new HashSet<String>();

    public Converter() {
    }

    public Converter(Configuration conf) {
      setConf(conf);
    }

    public void configure(JobConf conf) {

      try {

        // get the file system and the configuration file from the classpath
        this.conf = conf;
        FileSystem fs = FileSystem.get(conf);
        String configFile = conf.get("custom.fields.config",
          "custom-fields.xml");
        LOG.info("Reading configuration field configuration from " + configFile);
        Properties customFieldProps = new Properties();
        InputStream fis = conf.getConfResourceAsInputStream(configFile);
        if (fis == null) {
          throw new IOException("Was unable to open " + configFile);
        }

        // load the configuration file as properties
        customFieldProps.loadFromXML(fis);

        // loop through the properties setting field flags
        Enumeration propKeys = customFieldProps.keys();
        while (propKeys.hasMoreElements()) {
          String prop = (String)propKeys.nextElement();
          if (prop.endsWith(".name")) {
            String propName = prop.substring(0, prop.length() - 5);
            String name = customFieldProps.getProperty(prop);

            String indexedProp = customFieldProps.getProperty(propName
              + ".indexed");
            String storedProp = customFieldProps.getProperty(propName
              + ".stored");
            String tokProp = customFieldProps.getProperty(propName
              + ".tokenized");
            boolean indexed = (indexedProp.equalsIgnoreCase("yes")
              || indexedProp.equalsIgnoreCase("true") || indexedProp.equalsIgnoreCase("on"));
            boolean stored = (storedProp.equalsIgnoreCase("yes")
              || storedProp.equalsIgnoreCase("true") || storedProp.equalsIgnoreCase("on"));
            boolean tokenized = (tokProp.equalsIgnoreCase("yes")
              || tokProp.equalsIgnoreCase("true") || tokProp.equalsIgnoreCase("on"));
            boolean[] flags = {indexed, stored, tokenized};
            flagMap.put(name, flags);

            String multiProp = customFieldProps.getProperty(propName + ".multi");
            boolean multi = (multiProp.equalsIgnoreCase("yes")
              || multiProp.equalsIgnoreCase("true") || multiProp.equalsIgnoreCase("on"));
            if (multi) {
              multiFields.add(name);
            }
          }
        }
      }
      catch (Exception e) {
        LOG.error("Error loading custom field properties:\n"
          + StringUtils.stringifyException(e));
      }
    }

    public void map(LongWritable key, Text value,
      OutputCollector<Text, FieldWritable> output, Reporter reporter)
      throws IOException {

      // split the file on tabs
      String line = value.toString();
      String[] fields = line.split("\t");
      if (fields.length >= 3) {

        // fields must be in a specific order, default values for optional fields
        String url = fields[0];
        String fieldName = fields[1];
        String fieldVal = fields[2];
        String fieldScore = (fields.length > 3 ? fields[3] : null);
        String fieldType = (fields.length > 4 ? fields[4] : "CONTENT").toUpperCase();

        // creates the FieldWritable objects and collects
        boolean[] flags = flagMap.get(fieldName);
        if (flags != null) {
          FieldWritable field = null;
          if (fieldScore != null) {
            field = new FieldWritable(fieldName, fieldVal,
              FieldType.valueOf(fieldType), Float.parseFloat(fieldScore),
              flags[0], flags[1], flags[2]);
          }
          else {
            field = new FieldWritable(fieldName, fieldVal,
              FieldType.valueOf(fieldType), flags[0], flags[1], flags[2]);
          }
          output.collect(new Text(url), field);
        }
      }
    }

    public void reduce(Text key, Iterator<FieldWritable> values,
      OutputCollector<Text, FieldWritable> output, Reporter reporter)
      throws IOException {

      // if multiple fields are allowed collect all of them, if not allowed
      // and multiple fields are present all of the values are ignored
      Set<String> multiSet = new HashSet<String>();
      while (values.hasNext()) {
        FieldWritable field = values.next();
        String name = field.getName();
        boolean isMulti = multiFields.contains(name);
        if (isMulti || (!isMulti && !multiSet.contains(name))) {
          output.collect(key, field);
          multiSet.add(name);
        }
        else {
          LOG.info("Ignoring multiple " + name + " fields for "
            + key.toString());
        }
      }
    }

    public void close() {
    }
  }

  /**
   * Aggregates FieldWritable objects by the same name for the same URL.  These
   * objects are them filtered for multiple values against configuration 
   * settings.
   */
  public static class Collector
    extends Configured
    implements Mapper<Text, Writable, Text, ObjectWritable>,
    Reducer<Text, ObjectWritable, Text, FieldWritable> {

    private JobConf conf;

    public void configure(JobConf conf) {
      this.conf = conf;
    }

    public void close() {
    }

    public void map(Text key, Writable value,
      OutputCollector<Text, ObjectWritable> output, Reporter reporter)
      throws IOException {

      ObjectWritable objWrite = new ObjectWritable();
      objWrite.set(value);
      output.collect(key, objWrite);
    }

    public void reduce(Text key, Iterator<ObjectWritable> values,
      OutputCollector<Text, FieldWritable> output, Reporter reporter)
      throws IOException {

      FieldsWritable basicFields = null;
      List<FieldWritable> customFields = new ArrayList<FieldWritable>();

      while (values.hasNext()) {
        ObjectWritable objWrite = values.next();
        Object obj = objWrite.get();
        if (obj instanceof FieldWritable) {
          customFields.add((FieldWritable)obj);
        }
        else if (obj instanceof FieldsWritable) {
          basicFields = (FieldsWritable)obj;
        }
      }

      if (basicFields != null && customFields.size() > 0) {
        for (int i = 0; i < customFields.size(); i++) {
          output.collect(key, customFields.get(i));
        }
      }
    }
  }

  void createFields(Path basicFields, Path[] inputs, Path output)
    throws IOException {

    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);
    Path tempFields = new Path(output + "-"
      + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
    runConverter(inputs, tempFields);
    runCollector(basicFields, tempFields, output);
    fs.delete(tempFields, true);
  }

  public static void main(String[] args)
    throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new CustomFields(),
      args);
    System.exit(res);
  }

  /**
   * Runs the CustomFields job.
   */
  public int run(String[] args)
    throws Exception {

    Options options = new Options();
    Option helpOpts = OptionBuilder.withArgName("help").withDescription(
      "show this help message").create("help");
    Option outputOpts = OptionBuilder.withArgName("output").hasArg().withDescription(
      "the output index directory").create("output");
    Option inputOpts = OptionBuilder.withArgName("input").hasArgs().withDescription(
      "the input directories with text field files").create("input");
    Option basicFieldOpts = OptionBuilder.withArgName("basicfields").hasArg().withDescription(
      "the basicfields to use").create("basicfields");
    options.addOption(helpOpts);
    options.addOption(inputOpts);
    options.addOption(basicFieldOpts);
    options.addOption(outputOpts);

    CommandLineParser parser = new GnuParser();
    try {

      CommandLine line = parser.parse(options, args);
      if (line.hasOption("help") || !line.hasOption("output")
        || !line.hasOption("basicfields")) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("CustomFields", options);
        return -1;
      }

      String[] inputs = line.getOptionValues("input");
      Path[] inputPaths = new Path[inputs.length];
      for (int i = 0; i < inputs.length; i++) {
        inputPaths[i] = new Path(inputs[i]);
      }
      String output = line.getOptionValue("output");
      String basicFields = line.getOptionValue("basicfields");

      createFields(new Path(basicFields), inputPaths, new Path(output));
      return 0;
    }
    catch (Exception e) {
      LOG.fatal("CustomFields: " + StringUtils.stringifyException(e));
      return -2;
    }
  }
}
