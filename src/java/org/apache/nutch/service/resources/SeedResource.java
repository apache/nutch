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

import static javax.ws.rs.core.Response.status;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.collections.CollectionUtils;
import org.apache.nutch.service.model.request.SeedList;
import org.apache.nutch.service.model.request.SeedUrl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

@Path("/seed")
public class SeedResource extends AbstractResource {
  private static final Logger log = LoggerFactory
      .getLogger(AdminResource.class);

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
    if (seedList == null) {
      return Response.status(Status.BAD_REQUEST)
          .entity("Seed list cannot be empty!").build();
    }
    File seedFile = createSeedFile();
    BufferedWriter writer = getWriter(seedFile);

    Collection<SeedUrl> seedUrls = seedList.getSeedUrls();
    if (CollectionUtils.isNotEmpty(seedUrls)) {
      for (SeedUrl seedUrl : seedUrls) {
        writeUrl(writer, seedUrl);
      }
    }

    return Response.ok().entity(seedFile.getParent()).build();
  }

  private void writeUrl(BufferedWriter writer, SeedUrl seedUrl) {
    try {
      writer.write(seedUrl.getUrl());
      writer.newLine();
      writer.flush();
    } catch (IOException e) {
      throw handleException(e);
    }
  }

  private BufferedWriter getWriter(File seedFile) {
    try {
      return new BufferedWriter(new FileWriter(seedFile));
    } catch (FileNotFoundException e) {
      throw handleException(e);
    } catch (IOException e) {
      throw handleException(e);
    }
  }

  private File createSeedFile() {
    try {
      return File.createTempFile("seed", ".txt", Files.createTempDir());
    } catch (IOException e) {
      throw handleException(e);
    }
  }

  private RuntimeException handleException(Exception e) {
    log.error("Cannot create seed file!", e);
    return new WebApplicationException(status(Status.INTERNAL_SERVER_ERROR)
        .entity("Cannot create seed file!").build());
  }

}
