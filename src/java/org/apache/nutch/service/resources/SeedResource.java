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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.collections.CollectionUtils;
import org.apache.nutch.service.model.request.SeedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * used to create a seedlist (for the Nutch server) 
 * called seed.txt in the directory/folder provided by the SeedList object 
 * 
 *
 */

@Path(value = "/seed")
public class SeedResource extends AbstractResource {
	private static final Logger log = LoggerFactory
			.getLogger(SeedResource.class);

	@POST
	@Path(value = "/create")
	@Consumes(MediaType.APPLICATION_JSON)
	public boolean create(SeedList seedList) {
		if (seedList == null) {
			throw new WebApplicationException(Response.status(Status.BAD_REQUEST)
					.entity("Seed list cannot be empty!").build());
		}
		File seedFile = createSeedFile(seedList.getDirectory());
		BufferedWriter writer = getWriter(seedFile);
		Collection<String> seedUrls = seedList.getSeedUrls();
		if (CollectionUtils.isNotEmpty(seedUrls)) {
			for (String seedUrl : seedUrls) {
				writeUrl(writer, seedUrl);
			}
		}

		return true;
	}

	private void writeUrl(BufferedWriter writer, String seedUrl) {
		try {
			writer.write(seedUrl);
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

	private File createSeedFile(String directory) {
		try {
			java.nio.file.Path path=Paths.get(directory);
			if (!Files.exists(path)) {
				new File(directory).mkdir();
			}	
			File targetFile = new File(directory+"/seed.txt");
			return targetFile;
		} catch (Exception e) {
			throw handleException(e);
		}
	}

	private RuntimeException handleException(Exception e) {
		log.error("Cannot create seed file!", e);
		return new WebApplicationException(status(Status.INTERNAL_SERVER_ERROR)
				.entity("Cannot create seed file!").build());
	}

}