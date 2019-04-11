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
package org.apache.nutch.service.resources;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.service.impl.ServiceWorker;
import org.apache.nutch.service.model.request.ServiceConfig;
import org.apache.nutch.service.model.response.ServiceInfo;
import org.apache.nutch.tools.CommonCrawlDataDumper;

/**
 * The services resource defines an endpoint to enable the user to carry out
 * Nutch jobs like dump, commoncrawldump, etc.
 */
@Path("/services")
public class ServicesResource {

  private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

  @GET
  @Path("/commoncrawldump/{crawlId}")
  public Response listDumpPaths(@PathParam("crawlId") String crawlId) {
    File dumpFilePath = new File(crawlId + File.separator + "dump/");
    File dumpFileList[] = dumpFilePath.listFiles();
    List<String> fileNames = new ArrayList<>();
    if (dumpFileList != null) {
      for (File f : dumpFileList) {
        fileNames.add(f.getPath());
      }
    }
    ServiceInfo info = new ServiceInfo();
    info.setDumpPaths(fileNames);
    return Response.ok().entity(info).type(MediaType.APPLICATION_JSON).build();
  }

  @POST
  @Path("/commoncrawldump")
  public Response commoncrawlDump(ServiceConfig serviceConfig) {
    String crawlId = serviceConfig.getCrawlId();
    String outputDir = crawlId + File.separator + "dump" + File.separator
        + "commoncrawl-" + sdf.format(System.currentTimeMillis());

    Map<String, Object> args = serviceConfig.getArgs();
    args.put("outputDir", outputDir);
    if (!args.containsKey(Nutch.ARG_SEGMENTDIR)) {
      args.put("segment", crawlId + File.separator + "segments");
    }
    serviceConfig.setArgs(args);
    ServiceWorker worker = new ServiceWorker(serviceConfig,
        new CommonCrawlDataDumper());
    worker.run();

    return Response.ok(outputDir).type(MediaType.TEXT_PLAIN).build();
  }

}
