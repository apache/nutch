package org.apache.nutch.fetcher;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.BufferedWriter;
import java.lang.invoke.MethodHandles;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.AbstractFetchSchedule;
import org.apache.nutch.crawl.CrawlDBTestUtil;
import org.apache.nutch.crawl.Generator;
import org.apache.nutch.crawl.Injector;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.protocol.Content;
import org.eclipse.jetty.server.Server;
import org.apache.nutch.crawl.CrawlDatum;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestFetchWithParseFailures {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());
  
  private static final Path TEST_DIR = new Path("build/test/test-fail-parse");
  
  private static final String BASE_FOLDER = "build/test/data/fetch-parse-failure";
  private static final String TEST_FILE = "test.html";
  private static final String BKP_FILE = "test.html.bkp";
  private static final String HTML_BAD = "<html><body>This should not parse as <html...!";
  // unused good html, to check if 2 fetches are done: lines ~180-181
  private static final String HTML_GOOD = "<html><body>This should parse as html...!</body><html>";
  
  private Configuration conf;
  private FileSystem fs;
  private Path crawldbPath;
  private Path segmentsPath;
  private Path urlPath;
  private Server server;
  
  private static List<String> files;
  private static java.nio.file.Path baseFolderPath;
  
  private static ExecutorService executor = Executors.newCachedThreadPool();
  
  
  @BeforeEach
  public void setUp() throws Exception {
    baseFolderPath = java.nio.file.Paths.get(BASE_FOLDER);
    java.nio.file.Files.copy(baseFolderPath.resolve(TEST_FILE), baseFolderPath.getParent().resolve(BKP_FILE));
    
    files = java.nio.file.Files.list(baseFolderPath).map(p -> p.getFileName().toString()).filter(n -> !n.equals("robots.txt")).collect(Collectors.toList());
    Collections.sort(files);
    LOG.info("TEST FILES : " + files);
    
    conf = CrawlDBTestUtil.createContext().getConfiguration();
    conf.set("plugin.includes", "protocol-http|urlfilter-regex|parse-(html|tika)|index-(basic|anchor)|indexer-csv|scoring-opic|urlnormalizer-(pass|regex|basic)");
    //conf.setLong("db.fetch.interval.default", 1);
    //conf.setLong("db.fetch.interval.max", 1);
    //conf.setLong("fetcher.timelimit.mins", 2);
    //conf.setInt("fetcher.threads.start.delay", 1);
    conf.setInt("fetcher.threads.fetch", 1);
    conf.setBoolean("fetcher.parse", true);
    conf.setBoolean("fetcher.store.content", true);
    conf.setBoolean(Nutch.DELETE_FAILED_PARSE, true);
    
    fs = FileSystem.get(conf);
    fs.delete(TEST_DIR, true);
    crawldbPath = new Path(TEST_DIR, "crawldb");
    segmentsPath = new Path(TEST_DIR, "segments");
    urlPath = new Path(TEST_DIR, "urls");
    server = CrawlDBTestUtil.getServer(
        conf.getInt("content.server.port", 1234),
        BASE_FOLDER);
    server.start();
  }

  @AfterEach
  public void tearDown() throws Exception {
    executor.shutdown();
    server.stop();
    for (int i = 0; i < 5; i++) {
      if (!server.isStopped()) {
       Thread.sleep(1000);
      }
    }
    fs.delete(TEST_DIR, true);
    java.nio.file.Files.copy(baseFolderPath.getParent().resolve(BKP_FILE), baseFolderPath.resolve(TEST_FILE), StandardCopyOption.REPLACE_EXISTING);
    java.nio.file.Files.delete(baseFolderPath.getParent().resolve(BKP_FILE));
  }
  

  @Test
  //@Disabled("This test does not fully run 99 times out of 100, but still fails at line 246, instead of printing log message or failing on intermediate assertions.")
  public void testFetchWithParseFailure() throws Exception {
    AtomicInteger fetchCount= new AtomicInteger(0);
    AbstractFetchSchedule schedule = new AbstractFetchSchedule(conf) {};
    
    // generate seedlist
    ArrayList<String> urls = new ArrayList<String>();
    files.forEach(f -> urls.add("http://127.0.0.1:" + server.getURI().getPort() + "/" + f));
    CrawlDBTestUtil.generateSeedList(fs, urlPath, urls);

    // inject
    Injector injector = new Injector(conf);
    injector.inject(crawldbPath, urlPath);

    // generate
    Generator g1 = new Generator(conf);
    Path[] generatedSegment1 = g1.generate(crawldbPath, segmentsPath, 1,
        Long.MAX_VALUE, Long.MAX_VALUE, false, false, true, 1, null);
    Assertions.assertNotNull(generatedSegment1);
    
    Map<String, Object> args1 = Map.of("segment", generatedSegment1[0]);
    // fetch once
    LOG.info("1ST FETCH");
    Fetcher fetcher1 = new Fetcher(conf);
    Map<String, Object> result1 = executor.submit(new Callable<Map<String, Object>>(){

      @Override
      public Map<String, Object> call() throws Exception {
        try {
          return fetcher1.run(args1, "1");
        } catch (Throwable t) {
          LOG.error("ERROR!", t);
        } finally {
          fetchCount.incrementAndGet();
        }
        return null;
      }}).get();
    Assertions.assertFalse(result1.isEmpty());
    Assertions.assertEquals("0", result1.get(Nutch.VAL_RESULT));
    Assertions.assertEquals(1, fetchCount.get());
    
    // verify content
    LOG.info("1ST VERIFY CONTENT");
    Map<String, String> fetchedUrls = new HashMap<>();
    Path data = new Path(
        new Path(generatedSegment1[0], Content.DIR_NAME), "part-r-00000/data");
    try(SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(data))){
      do {
        Text key = new Text();
        Content value = new Content();
        if (!reader.next(key, value))
          break;
        // status is stored in Nutch.FETCH_STATUS_KEY: ref. FetcherThread
        String status = value.getMetadata().get(Nutch.FETCH_STATUS_KEY);
        fetchedUrls.put(key.toString(), status);
      } while (true);
    }
    assertEquals(urls.size(), fetchedUrls.size());
    LOG.info("1ST FETCHED URLS: {}", fetchedUrls);
    
    // change content to un-parseable
    try(BufferedWriter writer = 
        java.nio.file.Files.newBufferedWriter(baseFolderPath.resolve(TEST_FILE), StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)){
      writer.write(HTML_BAD);
      //writer.write(HTML_GOOD);
      writer.flush();
    }
    
    // force re-fetch and wait a bit
    urls.forEach(i -> 
      schedule.forceRefetch(new Text(i), new CrawlDatum(CrawlDatum.STATUS_DB_UNFETCHED, 1), true));
    long start = System.currentTimeMillis();
    while(System.currentTimeMillis() < start + 1000L);

    // inject and generate again
    injector.inject(crawldbPath, urlPath, true, true);
    Generator g2 = new Generator(conf);
    Path[] generatedSegment2 = g2.generate(crawldbPath, segmentsPath, 1,
        Long.MAX_VALUE, Long.MAX_VALUE, false, false, true, 1, null);
    Assertions.assertNotNull(generatedSegment2);


    Map<String, Object> args2 = Map.of("segment", generatedSegment2[0]);
    // next fetch should generate parse failure
    LOG.info("2ND FETCH");
    Fetcher fetcher2 = new Fetcher(conf);
    
    Map<String, Object> result2 = executor.submit(new Callable<Map<String, Object>>(){

      @Override
      public Map<String, Object> call() throws Exception {
        try {
          return fetcher2.run(args2, "2");
        } catch (Throwable t) {
          System.out.println("ERROR! " + t.getMessage() + " StackTrace: " + t.getStackTrace());
          LOG.error("ERROR!", t);
        } finally {
          fetchCount.incrementAndGet();
        }
        return null;
      }}).get();
    Assertions.assertFalse(result2.isEmpty());
    Assertions.assertEquals("0", result2.get(Nutch.VAL_RESULT));
    Assertions.assertEquals(2, fetchCount.get());
    
    /* Even if execution comes to this point, it does not print the next 2 System.out messages, 
     * but it continues, and fails the validation below.
     * Log for the 2nd fetch ends with the QueueFeeder queuing status" */

    // verify parsed data
    LOG.info("2ND VERIFY CONTENT");
    Path newData = new Path(
        new Path(generatedSegment2[0], Content.DIR_NAME), "part-r-00000/data");
    try(SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(newData))){
      do {
        Text key = new Text();
        Content value = new Content();
        if (!reader.next(key, value))
          break;
        // status is stored in Nutch.FETCH_STATUS_KEY: ref. FetcherThread
        String status = value.getMetadata().get(Nutch.FETCH_STATUS_KEY);
        fetchedUrls.put(key.toString(), status);
      } while (true);
    }
    assertEquals(urls.size(), fetchedUrls.size());
    LOG.info("2ND FETCHED STATUS: {}", fetchedUrls);
    
    for (Map.Entry<String, String> entry : fetchedUrls.entrySet()) {
      if(entry.getKey().endsWith(TEST_FILE)) {
        Assertions.assertEquals(entry.getValue(), ""+CrawlDatum.STATUS_PARSE_FAILED);
      } else {
        Assertions.assertEquals(entry.getValue(), ""+CrawlDatum.STATUS_FETCH_SUCCESS);
      }
    }
  }
}
