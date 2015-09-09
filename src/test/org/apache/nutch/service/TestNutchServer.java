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

package org.apache.nutch.service;

import javax.ws.rs.core.Response;

import org.apache.cxf.jaxrs.client.WebClient;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestNutchServer {

	private static final Logger LOG = LoggerFactory.getLogger(TestNutchServer.class);
	NutchServer server = NutchServer.getInstance();

	private int port[] = {8081, 9999, 9100, 8900};
	private final String ENDPOINT_ADDRESS = "http://localhost:";

	@Test
	public void testNutchServerStartup() {
		boolean isRunning = false;
		for(int i=0;i<port.length; i++) {
			try {
				startServer(port[i]);
				isRunning = true;
				break;
			}catch(Exception e) {
				LOG.info("Could not start server on port: {}. Tries remaining {}",port[i],port.length-i);
			}
		}
		if(!isRunning) {
			LOG.info("Could not start server, all ports in use");
		}
		else {
			LOG.info("Testing admin endpoint");
			WebClient client = WebClient.create(ENDPOINT_ADDRESS + server.getPort());
			Response response = client.path("admin").get();
			Assert.assertTrue(response.readEntity(String.class).contains("startDate"));
			response = client.path("stop").get();
			Assert.assertTrue(response.readEntity(String.class).contains("Stopping"));
		}
	}
	
	private void startServer(int port) throws Exception{
		NutchServer.setPort(port);
		NutchServer.startServer();
	}
}
