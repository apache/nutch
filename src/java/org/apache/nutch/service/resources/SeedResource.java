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
package org.apache.nutch.service.resources;

import java.io.OutputStream;
import java.util.Collection;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.nutch.service.NutchServer;
import org.apache.nutch.service.model.request.SeedList;
import org.apache.nutch.service.model.request.SeedUrl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path("/seed")
public class SeedResource extends AbstractResource {
  private static final Logger log = LoggerFactory
      .getLogger(AdminResource.class);

  /**
   * Gets the list of seedFiles already created 
   * @return
   */
  @GET
  @Path("/")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSeedLists() {
    Map<String, SeedList> seeds = NutchServer.getInstance().getSeedManager().getSeeds();
    if(seeds!=null) {
      return Response.ok(seeds).build();
    }
    else {
      return Response.ok().build();
    }
  }
  
  /**
   * Method creates seed list file and returns temporary directory path
   * @param seedList
   * @return
   */
  @POST
  @Path("/create")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.TEXT_PLAIN)
  public Response createSeedFile(SeedList seedList) {
    try {
    if (seedList == null) {
      return Response.status(Status.BAD_REQUEST)
          .entity("Seed list cannot be empty!").build();
    }
    Collection<SeedUrl> seedUrls = seedList.getSeedUrls();
    
    String seedFilePath = writeToSeedFile(seedUrls);
    seedList.setSeedFilePath(seedFilePath);
    NutchServer.getInstance().getSeedManager().
          setSeedList(seedList.getName(), seedList);
    return Response.ok().entity(seedFilePath).build();
    } catch (Exception e) {
      log.warn("Error while creating seed : {}", e.getMessage());
    }
    return Response.serverError().build();
  }

  private String writeToSeedFile(Collection<SeedUrl> seedUrls) throws Exception {
    String seedFilePath = "seedFiles/seed-" + System.currentTimeMillis();
    org.apache.hadoop.fs.Path seedFolder = new org.apache.hadoop.fs.Path(seedFilePath);
    FileSystem fs = FileSystem.get(new Configuration());
    if(!fs.exists(seedFolder)) {
      if(!fs.mkdirs(seedFolder)) {
        throw new Exception("Could not create seed folder at : " + seedFolder);
      }
    }
    String filename = seedFilePath + System.getProperty("file.separator") + "urls";
    org.apache.hadoop.fs.Path seedPath = new org.apache.hadoop.fs.Path(filename);
    OutputStream os = fs.create(seedPath);
    if (CollectionUtils.isNotEmpty(seedUrls)) {
      for (SeedUrl seedUrl : seedUrls) {
        os.write(seedUrl.getUrl().getBytes());
        os.write("\n".getBytes());
      }
    }
    os.close();
    return seedPath.getParent().toString();
  }
}
