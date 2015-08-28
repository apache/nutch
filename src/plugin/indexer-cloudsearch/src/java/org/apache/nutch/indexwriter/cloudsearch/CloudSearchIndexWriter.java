/**
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

package org.apache.nutch.indexwriter.cloudsearch;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.nutch.indexer.IndexWriter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.NutchField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.cloudsearchdomain.AmazonCloudSearchDomainClient;
import com.amazonaws.services.cloudsearchdomain.model.ContentType;
import com.amazonaws.services.cloudsearchdomain.model.UploadDocumentsRequest;
import com.amazonaws.services.cloudsearchdomain.model.UploadDocumentsResult;
import com.amazonaws.services.cloudsearchv2.AmazonCloudSearchClient;
import com.amazonaws.services.cloudsearchv2.model.DescribeDomainsRequest;
import com.amazonaws.services.cloudsearchv2.model.DescribeDomainsResult;
import com.amazonaws.services.cloudsearchv2.model.DescribeIndexFieldsRequest;
import com.amazonaws.services.cloudsearchv2.model.DescribeIndexFieldsResult;
import com.amazonaws.services.cloudsearchv2.model.DomainStatus;
import com.amazonaws.services.cloudsearchv2.model.IndexFieldStatus;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;

/**
 * Writes documents to CloudSearch.
 */
public class CloudSearchIndexWriter implements IndexWriter {
  public static final Logger LOG = LoggerFactory
      .getLogger(CloudSearchIndexWriter.class);

  private static final int MAX_SIZE_BATCH_BYTES = 5242880;
  private static final int MAX_SIZE_DOC_BYTES = 1048576;

  private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(
      "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

  private AmazonCloudSearchDomainClient client;

  private int maxDocsInBatch = -1;

  private StringBuffer buffer;

  private int numDocsInBatch = 0;

  private boolean dumpBatchFilesToTemp = false;

  private Configuration conf;

  private Map<String, String> csfields = new HashMap<String, String>();

  private String regionName;

  @Override
  public void open(JobConf job, String name) throws IOException {
    LOG.debug("CloudSearchIndexWriter.open() name={} ", name);

    maxDocsInBatch = job.getInt(CloudSearchConstants.MAX_DOCS_BATCH, -1);

    buffer = new StringBuffer(MAX_SIZE_BATCH_BYTES).append('[');

    dumpBatchFilesToTemp = job.getBoolean(CloudSearchConstants.BATCH_DUMP,
        false);

    if (dumpBatchFilesToTemp) {
      // only dumping to local file
      // no more config required
      return;
    }

    String endpoint = job.get(CloudSearchConstants.ENDPOINT);

    if (StringUtils.isBlank(endpoint)) {
      throw new RuntimeException("endpoint not set for CloudSearch");
    }

    AmazonCloudSearchClient cl = new AmazonCloudSearchClient();
    if (StringUtils.isNotBlank(regionName)) {
      cl.setRegion(RegionUtils.getRegion(regionName));
    }

    String domainName = null;

    // retrieve the domain name
    DescribeDomainsResult domains = cl
        .describeDomains(new DescribeDomainsRequest());

    Iterator<DomainStatus> dsiter = domains.getDomainStatusList().iterator();
    while (dsiter.hasNext()) {
      DomainStatus ds = dsiter.next();
      if (ds.getDocService().getEndpoint().equals(endpoint)) {
        domainName = ds.getDomainName();
        break;
      }
    }

    // check domain name
    if (StringUtils.isBlank(domainName)) {
      throw new RuntimeException(
          "No domain name found for CloudSearch endpoint");
    }

    DescribeIndexFieldsResult indexDescription = cl.describeIndexFields(
        new DescribeIndexFieldsRequest().withDomainName(domainName));
    for (IndexFieldStatus ifs : indexDescription.getIndexFields()) {
      String indexname = ifs.getOptions().getIndexFieldName();
      String indextype = ifs.getOptions().getIndexFieldType();
      LOG.info("CloudSearch index name {} of type {}", indexname, indextype);
      csfields.put(indexname, indextype);
    }

    client = new AmazonCloudSearchDomainClient();
    client.setEndpoint(endpoint);

  }

  @Override
  public void delete(String url) throws IOException {

    try {
      JSONObject doc_builder = new JSONObject();

      doc_builder.put("type", "delete");

      // generate the id from the url
      String ID = CloudSearchUtils.getID(url);
      doc_builder.put("id", ID);

      // add to the batch
      addToBatch(doc_builder.toString(2), url);

    } catch (JSONException e) {
      LOG.error("Exception caught while building JSON object", e);
    }

  }

  @Override
  public void update(NutchDocument doc) throws IOException {
    write(doc);
  }

  @Override
  public void write(NutchDocument doc) throws IOException {
    try {
      JSONObject doc_builder = new JSONObject();

      doc_builder.put("type", "add");

      String url = doc.getField("url").toString();

      // generate the id from the url
      String ID = CloudSearchUtils.getID(url);
      doc_builder.put("id", ID);

      JSONObject fields = new JSONObject();

      for (final Entry<String, NutchField> e : doc) {
        String fieldname = cleanFieldName(e.getKey());
        String type = csfields.get(fieldname);

        // undefined in index
        if (!dumpBatchFilesToTemp && type == null) {
          LOG.info(
              "Field {} not defined in CloudSearch domain for {} - skipping.",
              fieldname, url);
          continue;
        }

        List<Object> values = e.getValue().getValues();
        // write the values
        for (Object value : values) {
          // Convert dates to an integer
          if (value instanceof Date) {
            Date d = (Date) value;
            value = DATE_FORMAT.format(d);
          }
          // normalise strings
          else if (value instanceof String) {
            value = CloudSearchUtils.stripNonCharCodepoints((String) value);
          }

          fields.accumulate(fieldname, value);
        }
      }

      doc_builder.put("fields", fields);

      addToBatch(doc_builder.toString(2), url);

    } catch (JSONException e) {
      LOG.error("Exception caught while building JSON object", e);
    }
  }

  private void addToBatch(String currentDoc, String url) throws IOException {
    int currentDocLength = currentDoc.getBytes(StandardCharsets.UTF_8).length;

    // check that the doc is not too large -> skip it if it does
    if (currentDocLength > MAX_SIZE_DOC_BYTES) {
      LOG.error("Doc too large. currentDoc.length {} : {}", currentDocLength,
          url);
      return;
    }

    int currentBufferLength = buffer.toString()
        .getBytes(StandardCharsets.UTF_8).length;

    LOG.debug("currentDoc.length {}, buffer length {}", currentDocLength,
        currentBufferLength);

    // can add it to the buffer without overflowing?
    if (currentDocLength + 2 + currentBufferLength < MAX_SIZE_BATCH_BYTES) {
      if (numDocsInBatch != 0)
        buffer.append(',');
      buffer.append(currentDoc);
      numDocsInBatch++;
    }
    // flush the previous batch and create a new one with this doc
    else {
      commit();
      buffer.append(currentDoc);
      numDocsInBatch++;
    }

    // have we reached the max number of docs in a batch after adding
    // this doc?
    if (maxDocsInBatch > 0 && numDocsInBatch == maxDocsInBatch) {
      commit();
    }
  }

  @Override
  public void commit() throws IOException {

    // nothing to do
    if (numDocsInBatch == 0) {
      return;
    }

    // close the array
    buffer.append(']');

    LOG.info("Sending {} docs to CloudSearch", numDocsInBatch);

    byte[] bb = buffer.toString().getBytes(StandardCharsets.UTF_8);

    if (dumpBatchFilesToTemp) {
      try {
        File temp = File.createTempFile("CloudSearch_", ".json");
        FileUtils.writeByteArrayToFile(temp, bb);
        LOG.info("Wrote batch file {}", temp.getName());
      } catch (IOException e1) {
        LOG.error("Exception while generating batch file", e1);
      } finally {
        // reset buffer and doc counter
        buffer = new StringBuffer(MAX_SIZE_BATCH_BYTES).append('[');
        numDocsInBatch = 0;
      }
      return;
    }
    // not in debug mode
    try (InputStream inputStream = new ByteArrayInputStream(bb)) {
      UploadDocumentsRequest batch = new UploadDocumentsRequest();
      batch.setContentLength((long) bb.length);
      batch.setContentType(ContentType.Applicationjson);
      batch.setDocuments(inputStream);
      UploadDocumentsResult result = client.uploadDocuments(batch);
    } catch (Exception e) {
      LOG.error("Exception while sending batch", e);
      LOG.error(buffer.toString());
    } finally {
      // reset buffer and doc counter
      buffer = new StringBuffer(MAX_SIZE_BATCH_BYTES).append('[');
      numDocsInBatch = 0;
    }
  }

  @Override
  public void close() throws IOException {
    // This will flush any unsent documents.
    commit();
    // close the client
    if (client != null){
      client.shutdown();
    }
  }

  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    String endpoint = getConf().get(CloudSearchConstants.ENDPOINT);
    boolean dumpBatchFilesToTemp = getConf()
        .getBoolean(CloudSearchConstants.BATCH_DUMP, false);
    this.regionName = getConf().get(CloudSearchConstants.REGION);

    if (StringUtils.isBlank(endpoint) && !dumpBatchFilesToTemp) {
      String message = "Missing CloudSearch endpoint. Should set it set via -D "
          + CloudSearchConstants.ENDPOINT + " or in nutch-site.xml";
      message += "\n" + describe();
      LOG.error(message);
      throw new RuntimeException(message);
    }
  }

  public String describe() {
    String configuredEndpoint = null;
    String configuredRegion = null;

    // get the values set in the conf
    if (getConf() != null) {
      configuredEndpoint = getConf().get(CloudSearchConstants.ENDPOINT);
      configuredRegion = getConf().get(CloudSearchConstants.REGION);
    }

    StringBuffer sb = new StringBuffer("CloudSearchIndexWriter\n");
    sb.append("\t").append(CloudSearchConstants.ENDPOINT)
        .append(" : URL of the CloudSearch domain's document endpoint.");
    if (StringUtils.isNotBlank(configuredEndpoint)) {
      sb.append(" (value: ").append(configuredEndpoint).append(")");
    }
    sb.append("\n");

    sb.append("\t").append(CloudSearchConstants.REGION)
        .append(" : name of the CloudSearch region.");
    if (StringUtils.isNotBlank(configuredRegion)) {
      sb.append(" (").append(configuredRegion).append(")");
    }
    sb.append("\n");
    return sb.toString();
  }

  /**
   * Remove the non-cloudSearch-legal characters. Note that this might convert
   * two fields to the same name.
   * 
   * @param name
   * @return
   */
  String cleanFieldName(String name) {
    String lowercase = name.toLowerCase();
    return lowercase.replaceAll("[^a-z_0-9]", "_");
  }

}
