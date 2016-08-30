/*******************************************************************************
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
 ******************************************************************************/
package org.apache.nutch.api.resources;

import static javax.ws.rs.core.Response.status;

import java.io.BufferedWriter;
import java.io.File;
import java.io.OutputStreamWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;

import org.apache.commons.collections.CollectionUtils;
import org.apache.nutch.api.model.request.SeedList;
import org.apache.nutch.api.model.request.SeedUrl;
import org.apache.nutch.api.security.SecurityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

@Path("/seed")
public class SeedResource extends AbstractResource {
  private static final Logger log = LoggerFactory.getLogger(SeedResource.class);

  @Context
  SecurityContext securityContext;

  @POST
  @Path("/create")
  @Consumes(MediaType.APPLICATION_JSON)
  /**
   * Method creates seed list file and returns temporary directory path
   * @param seedList
   * @return
   */
  public String createSeedFile(SeedList seedList) {
    SecurityUtils.allowOnlyAdmin(securityContext);
    if (seedList == null) {
      throw new WebApplicationException(Response.status(Status.BAD_REQUEST)
          .entity("Seed list cannot be empty!").build());
    }
    File seedFile = createSeedFile();
    BufferedWriter writer = getWriter(seedFile);

    Collection<SeedUrl> seedUrls = seedList.getSeedUrls();
    if (CollectionUtils.isNotEmpty(seedUrls)) {
      for (SeedUrl seedUrl : seedUrls) {
        writeUrl(writer, seedUrl);
      }
    }

    return seedFile.getParent();
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
      return new BufferedWriter(new OutputStreamWriter(new FileOutputStream(seedFile), StandardCharsets.UTF_8));
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
