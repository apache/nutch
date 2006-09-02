package org.apache.nutch.crawl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.UTF8;

import junit.framework.TestCase;

/**
 * Basic injector test:
 * 1. Creates a text file with urls
 * 2. Injects them into crawldb
 * 3. Reads crawldb entries and verifies contents
 * 4. Injects more urls into webdb
 * 5. Reads crawldb entries and verifies contents
 * 
 * @author nutch-dev <nutch-dev at lucene.apache.org>
 */
public class TestInjector extends TestCase {

  private FSDataOutputStream out;
  private Configuration conf;
  private FileSystem fs;
  final static Path testdir=new Path("build/test/inject-test");
  Path crawldbPath;
  Path urlPath;
  
  protected void setUp() throws Exception {
    conf = CrawlDBTestUtil.create();
    urlPath=new Path(testdir,"urls");
    crawldbPath=new Path(testdir,"crawldb");
    fs=FileSystem.get(conf);
    
  }
  
  protected void tearDown() throws IOException{
    fs.delete(testdir);
  }

  public void testInject() throws IOException {
    ArrayList<String> urls=new ArrayList<String>();
    for(int i=0;i<100;i++) {
      urls.add("http://zzz/" + i + ".html");
    }
    generateSeedList(urls);
    
    Injector injector=new Injector(conf);
    injector.inject(crawldbPath, urlPath);
    
    // verify results
    List<String>read=readCrawldb();
    
    Collections.sort(read);
    Collections.sort(urls);

    assertEquals(urls.size(), read.size());
    
    assertTrue(read.containsAll(urls));
    assertTrue(urls.containsAll(read));
    
    //inject more urls
    ArrayList<String> urls2=new ArrayList<String>();
    for(int i=0;i<100;i++) {
      urls2.add("http://xxx/" + i + ".html");
    }
    generateSeedList(urls2);
    injector.inject(crawldbPath, urlPath);
    urls.addAll(urls2);
    
    // verify results
    read=readCrawldb();
    

    Collections.sort(read);
    Collections.sort(urls);

    assertEquals(urls.size(), read.size());
    
    assertTrue(read.containsAll(urls));
    assertTrue(urls.containsAll(read));
    
  }
  
  /**
   * Generate seedlist
   * @throws IOException 
   */
  private void generateSeedList(List<String> contents) throws IOException{
    Path file=new Path(urlPath,"urls.txt");
    fs.mkdirs(urlPath);
    out=fs.create(file);
    Iterator<String> iterator=contents.iterator();
    while(iterator.hasNext()){
      String url=iterator.next();
      out.writeBytes(url);
      out.writeBytes("\n");
    }
    out.flush();
    out.close();
  }
  
  private List<String> readCrawldb() throws IOException{
    Path dbfile=new Path(crawldbPath,CrawlDatum.DB_DIR_NAME + "/part-00000/data");
    System.out.println("reading:" + dbfile);
    SequenceFile.Reader reader=new SequenceFile.Reader(fs, dbfile, conf);
    ArrayList<String> read=new ArrayList<String>();
    
    READ:
      do {
      UTF8 key=new UTF8();
      CrawlDatum value=new CrawlDatum();
      if(!reader.next(key, value)) break READ;
      read.add(key.toString());
    } while(true);

    return read;
  }

}
