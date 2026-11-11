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
package org.apache.nutch.tools;

import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metrics.ErrorTracker;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 *
 * Test harness for the {@link CommonCrawlDataDumper}.
 *
 */
public class TestCommonCrawlDataDumper {

  @Test
  public void testDump() throws Exception {
    File sampleSegmentDir = new File(System.getProperty("test.build.data",
        "."), "test-segments");
    File tempDir = Files.createTempDirectory("temp").toFile();

    String[] crawledFiles = {
        "c463a4381eb837f9f5d45978cfbde79e_.html",
        "a974b8d74f7779ab6c6f90b9b279467e_.html",
        "6bc6497314656a3129732efd708e9f96_.html",
        "6e88c40abe26cad0a726102997aed048_.html",
        "5cafdd88f4e9cf3f0cd4c298c6873358_apachecon-europe.html",
        "932dc10a76e894a2baa8ea4086ad72a8_apachecon-north-america.html",
        "8540187d75b9cd405b8fa97d665f9f90_.html",
        "e501bc976c8693b4d28a55b79c390a32_.html",
        "6add662f9f5758b7d75eec5cfa1f340b_.html",
        "d4f20df3c37033dc516067ee1f424e4e_.html",
        "d7b8fa9a02cdc95546030d04be4a98f3_solr.html",
        "3cbe876e3a8e7a397811de3bb6a945cd_.html",
        "5b987dde0da79d7f2e3f22b46437f514_bot.html",
        "3d742820d9a701a1f02e10d5bf5ae633_credits.html",
        "693673f3c73d04a26276effdea69b7ee_downloads.html",
        "4f7e3469dafabb4c3b87b00531f81aa4_index.html",
        "15c5330675be8a69995aab18ff9859e0_javadoc.html",
        "bc624e1b49e29870ef095819bb0e977a_mailing_lists.html",
        "a7d66b68754c3665c66e62225255e3fd_version_control.html",
        "32fb7fe362e1a0d8a1b15addf2a00bdc_1.9-rel",
        "54ab3db10fe7b26415a04e21045125a8_1zE.html",
        "1012a41c08092c40340598bd8ee0bfa6_PGa.html",
        "c830cfc5c28bed10e69d5b83e9c1bcdc_nutch_2.3",
        "687d915dc264a77f35c61ba841936730_oHY.html",
        "2bf1afb650010128b4cf4afe677db3c5_1pav9xl.html",
        "550cab79e14110bbee61c36c61c830b0_1pbE15n.html",
        "664ff07b46520cc1414494ae49da91f6_.html",
        "04223714e648a6a43d7c8af8b095f733_.html",
        "3c8ccb865cd72cca06635d74c7f2f3c4_.html",
        "90fe47b28716a2230c5122c83f0b8562_Becoming_A_Nutch_Developer.html",
        "ac0fefe70007d40644e2b8bd5da3c305_FAQ.html",
        "bc9bc7f11c1262e8924032ab1c7ce112_NutchPropertiesCompleteList.html",
        "78d04611985e7375b441e478fa36f610_.html",
        "64adaebadd44e487a8b58894e979dc70_CHANGES.txt",
        "a48e9c2659b703fdea3ad332877708d8_.html",
        "159d66d679dd4442d2d8ffe6a83b2912_sponsorship.html",
        "66f1ce6872c9195c665fc8bdde95f6dc_thanks.html",
        "ef7ee7e929a048c4a119af78492095b3_.html",
        "e4251896a982c2b2b68678b5c9c57f4d_.html",
        "5384764a16fab767ebcbc17d87758a24_.html",
        "a6ba75a218ef2a09d189cb7dffcecc0f_.html",
        "f2fa63bd7a3aca63841eed4cd10fb519_SolrCloud.html",
        "f8de0fbda874e1a140f1b07dcebab374_NUTCH-1047.html",
        "9c120e94f52d690e9cfd044c34134649_NUTCH-1591.html",
        "7dd70378379aa452279ce9200d0a5fed_NUTCH-841.html",
        "ddf78b1fe5c268d59fd62bc745815b92_.html",
        "401c9f04887dbbf8d29ad52841b8bdb3_ApacheNutch.html",
        "8f984e2d3c2ba68d1695288f1738deaf_Nutch.html",
        "c2ef09a95a956207cea073a515172be2_FrontPage.html",
    "90d9b76e8eabdab1cbcc29bea437c7ae_NutchRESTAPI.html" };

    CommonCrawlDataDumper dumper = new CommonCrawlDataDumper(
        new CommonCrawlConfig());
    dumper.dump(tempDir, sampleSegmentDir, null, false, null, false, "", false);

    Collection<File> tempFiles = FileUtils.listFiles(tempDir,
        FileFilterUtils.fileFileFilter(),
        FileFilterUtils.directoryFileFilter());

    for (String expectedFileName : crawledFiles) {
      assertTrue(hasFile(expectedFileName, tempFiles),
          "Missed file " + expectedFileName + " in dump");
    }

  }

  @Test
  public void testDumpWithNoSegmentDirectoriesRecordsOtherError() throws Exception {
    File emptySegmentDir = Files.createTempDirectory("empty-segments").toFile();
    File outputDir = Files.createTempDirectory("dump-output").toFile();

    ErrorTracker mockErrorTracker = mock(ErrorTracker.class);
    CommonCrawlDataDumper dumper = new CommonCrawlDataDumper();
    Field errorTrackerField = CommonCrawlDataDumper.class.getDeclaredField("errorTracker");
    errorTrackerField.setAccessible(true);
    errorTrackerField.set(dumper, mockErrorTracker);

    dumper.dump(outputDir, emptySegmentDir, null, false, null, false, "", false);

    verify(mockErrorTracker, times(1)).recordError(ErrorTracker.ErrorType.OTHER);
  }

  /**
   * With gzip and epoch filenames both enabled, CommonCrawlDataDumper copies
   * the crawled metadata key "date" into each tar entry name. That value
   * comes from a response header a remote server controls, so it must not be
   * able to change the directory an entry is written under.
   *
   * What this test does:
   * <ol>
   * <li>Writes one segment containing two documents. The first document has
   * the date "../../../tmp/evil/x", a parent-directory path. The second has
   * the normal HTTP date "Thu, 25 Sep 2014 04:16:58 GMT".</li>
   * <li>Runs {@code dump} with gzip and epoch filenames enabled, which is the
   * code path that builds entry names from the date.</li>
   * <li>Reads the entry names back from the resulting .tar.gz. The archive is
   * never extracted, so the test writes nothing outside its temporary
   * directories.</li>
   * <li>For every entry, checks that the name has no ".." segment, is not
   * absolute, and that its final path segment has no slash, no backslash, and
   * none of the unsafe date text.</li>
   * <li>Checks that the normal HTTP date still appears in an entry name. This
   * stops the sanitizer from rewriting legitimate dates.</li>
   * </ol>
   *
   * Why: a regression here would let a crawled site choose where archive
   * entries land, and the failure would show up only later, when someone
   * unpacks the archive. Checking the names directly catches the problem at
   * the point where Nutch writes them.
   */
  @Test
  public void testEpochGzipEntryNamesIgnoreUnsafeDateMetadata()
      throws Exception {
    String benignDate = "Thu, 25 Sep 2014 04:16:58 GMT";
    String unsafeDate = "../../../tmp/evil/x";
    File segmentDir = Files.createTempDirectory("epoch-segment").toFile();
    File outputDir = Files.createTempDirectory("epoch-output").toFile();
    writeContentSegment(segmentDir,
        "http://example.test/unsafe.html", unsafeDate,
        "http://example.test/benign.html", benignDate);

    CommonCrawlDataDumper dumper = new CommonCrawlDataDumper(
        new CommonCrawlConfig());
    dumper.dump(outputDir, segmentDir, null, true, null, true, "", false);

    List<String> entryNames = readTarEntryNames(outputDir);
    assertFalse(entryNames.isEmpty(), "gzip epoch dump wrote no tar entries");
    boolean sawBenign = false;
    for (String name : entryNames) {
      assertFalse(name.contains(".."), "entry name contains ..: " + name);
      assertFalse(name.startsWith("/"), "entry name is absolute: " + name);
      String segment = name.substring(name.lastIndexOf('/') + 1);
      assertFalse(segment.contains(".."), segment);
      assertFalse(segment.indexOf('/') >= 0, segment);
      assertFalse(segment.indexOf('\\') >= 0, segment);
      assertFalse(segment.contains(unsafeDate), segment);
      if (name.contains(benignDate)) {
        sawBenign = true;
      }
    }
    assertTrue(sawBenign, "normal HTTP date was not kept in an entry name");
  }

  /**
   * Writes the two test documents as a single SequenceFile at
   * {@code segment/content/part-00000/data}. That is the layout the dumper
   * scans for, so the file must sit in exactly this location. Each document
   * is stored with its own URL and a metadata record holding only the
   * lowercase "date" key.
   */
  private void writeContentSegment(File segmentDir, String unsafeUrl,
      String unsafeDate, String benignUrl, String benignDate) throws Exception {
    File partDir = new File(segmentDir, "content/part-00000");
    assertTrue(partDir.mkdirs(), "could not create " + partDir);
    Configuration conf = NutchConfiguration.create();
    Path data = new Path(new File(partDir, "data").getAbsolutePath());
    byte[] body = "<html><body>example</body></html>"
        .getBytes(StandardCharsets.UTF_8);
    try (SequenceFile.Writer writer = SequenceFile.createWriter(conf,
        SequenceFile.Writer.file(data),
        SequenceFile.Writer.keyClass(Text.class),
        SequenceFile.Writer.valueClass(Content.class))) {
      writer.append(new Text(unsafeUrl),
          contentWithDate(unsafeUrl, unsafeDate, body, conf));
      writer.append(new Text(benignUrl),
          contentWithDate(benignUrl, benignDate, body, conf));
    }
  }

  /**
   * Builds a Content record whose metadata has only the lowercase key "date".
   * The dumper reads that exact key when it builds the entry name, so using a
   * different case would bypass the code path under test.
   */
  private Content contentWithDate(String url, String date, byte[] body,
      Configuration conf) {
    Metadata metadata = new Metadata();
    metadata.set("date", date);
    return new Content(url, url, body, "text/html", metadata, conf);
  }

  /**
   * Finds the single .tar.gz that the dump wrote into {@code outputDir} and
   * returns every entry name it contains. Fails the test if no archive was
   * written. Entry names are read as text only; nothing is extracted.
   */
  private List<String> readTarEntryNames(File outputDir) throws Exception {
    File archive = null;
    File[] files = outputDir.listFiles();
    if (files != null) {
      for (File file : files) {
        if (file.getName().endsWith(".tar.gz")) {
          archive = file;
          break;
        }
      }
    }
    assertTrue(archive != null && archive.isFile(),
        "gzip epoch dump did not write a tar.gz");
    List<String> names = new ArrayList<>();
    try (TarArchiveInputStream tar = new TarArchiveInputStream(
        new GzipCompressorInputStream(new FileInputStream(archive)))) {
      TarArchiveEntry entry;
      while ((entry = tar.getNextEntry()) != null) {
        names.add(entry.getName());
      }
    }
    return names;
  }

  private boolean hasFile(String fileName, Collection<File> files) {
    for (File f : files) {
      if (f.getName().equals(fileName)) {
        return true;
      }
    }
    return false;
  }
}
