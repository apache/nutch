package org.apache.nutch.fetcher;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

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
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolFactory;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.CrawlDb;
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
  
  private Configuration conf;
  private FileSystem fs;
  private Path crawldbPath;
  private Path segmentsPath;
  private Path urlPath;
  private Server server;
  
  private static List<String> files;
  private static java.nio.file.Path baseFolderPath;
  
  private static ExecutorService executor = Executors.newCachedThreadPool();
  private static final AtomicInteger FETCH_COUNT = new AtomicInteger(0);
  
  
  @BeforeEach
  public void setUp() throws Exception {
    baseFolderPath = java.nio.file.Paths.get(BASE_FOLDER);
    
    files = java.nio.file.Files.list(baseFolderPath).map(p -> p.getFileName().toString()).filter(n -> !n.equals("robots.txt")).collect(Collectors.toList());
    Collections.sort(files);
    LOG.info("TEST FILES : " + files);
    
    conf = CrawlDBTestUtil.createContext().getConfiguration();
    conf.set("plugin.includes", "protocol-http|urlfilter-regex|parse-html|index-(basic|anchor)|indexer-csv|scoring-opic|urlnormalizer-(pass|regex|basic)");
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
        BASE_FOLDER, new ParseFailureResourceHandler());
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
  }
  

  @Test
  // Ref: full hadoop.log in logs/hadoop.log
  @Disabled("The default HTML Parser (Neko) is a lot more tolerant than expected.  Even random bytes in an HTML file do not produce a parsing error.")
  public void testFetchWithParseFailure() throws Exception {
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
          FETCH_COUNT.incrementAndGet();
        }
        return null;
      }}).get();
    Assertions.assertFalse(result1.isEmpty());
    Assertions.assertEquals("0", result1.get(Nutch.VAL_RESULT));
    Assertions.assertEquals(1, FETCH_COUNT.get());
    
    CrawlDb crawlDb = new CrawlDb(conf);
    crawlDb.update(crawldbPath, generatedSegment1, true, true, true, true);
    
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
    
//    // Validate what is now served:
//    String urlString = "http://127.0.0.1:55000/test.html";
//    Protocol protocol = new ProtocolFactory(conf).getProtocol(urlString);
//    Content content = protocol.getProtocolOutput(new Text(urlString), new CrawlDatum()).getContent();
//    String contentStr = new String(content.getContent());
//    LOG.info("TEST CONTENT FROM SERVER: {}", contentStr); // makes the log file unreadable
    
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

    crawlDb.update(crawldbPath, generatedSegment2, true, true, true, true);

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
          LOG.error("ERROR!", t);
        } finally {
          FETCH_COUNT.incrementAndGet();
        }
        return null;
      }}).get();
    Assertions.assertFalse(result2.isEmpty());
    Assertions.assertEquals("0", result2.get(Nutch.VAL_RESULT));
    Assertions.assertEquals(2, FETCH_COUNT.get());

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
  
  public static class ParseFailureResourceHandler extends ResourceHandler {
    
    public ParseFailureResourceHandler() {
      super();
    }
    
    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) 
        throws IOException, ServletException {
      if(FETCH_COUNT.get() == 1 && target.endsWith(TEST_FILE)) {
        LOG.info("FEEDING RANDOM BYTES INTO THE RESPONSE");
        response.setContentType("text/html");
        response.setContentLength(100); // wrong length on purpose

        byte[] randomBytes = new byte[1024];
        new Random(123).nextBytes(randomBytes);
        response.getOutputStream().write(randomBytes);
        
      } else {
        super.handle(target, baseRequest, request, response);
      }
    }
  }
}
