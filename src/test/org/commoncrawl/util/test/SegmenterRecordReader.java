package org.commoncrawl.util.test;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;

import java.util.Arrays;

public class SegmenterRecordReader extends Configured implements Tool {

    private Content content;

    @Override
    public int run(String[] args) throws Exception {
        return run(args[0], args[1]);
    }

    private int run(String path, String url) throws Exception {
        Path p = new Path(path, Content.DIR_NAME);
        Text k = new Text(url);
        MapFile.Reader[] readers = MapFileOutputFormat.getReaders(p, getConf());
        Content c = new Content();
        readers[0].get(k, c);
        assert (c.getUrl().equals(url));
        assert (c.getContent() == null || c.getContent().length == 0);
        this.content = c;

        return 0;
    }

    public static Content retrieveContent(String segmentPath, String url) throws Exception {
        SegmenterRecordReader reader = new SegmenterRecordReader();
        ToolRunner.run(NutchConfiguration.create(),
              reader, Arrays.asList(segmentPath, url).toArray(new String[0]));

        return reader.getContent();
    }

    public Content getContent() {
        return content;
    }

    public void setContent(Content content) {
        this.content = content;
    }
}
