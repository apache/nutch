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


import java.util.Map;
import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.nutch.service.model.request.NutchConfig;
import com.fasterxml.jackson.jaxrs.annotation.JacksonFeatures;
import com.fasterxml.jackson.databind.SerializationFeature;

@Path("/config")
public class ConfigResource extends AbstractResource{

  public static final String DEFAULT = "default";

  /**
   * Returns a list of all configurations created.
   * @return List of configurations
   */
  @GET
  @Path("/")
	@JacksonFeatures(serializationEnable =  { SerializationFeature.INDENT_OUTPUT })
  public Set<String> getConfigs() {
    return configManager.list();
  }

  /** 
   * Get configuration properties 
   * @param configId The configuration ID to fetch
   * @return HashMap of the properties set within the given configId
   */
  @GET
  @Path("/{configId}")
	@JacksonFeatures(serializationEnable =  { SerializationFeature.INDENT_OUTPUT })
  public Map<String, String> getConfig(@PathParam("configId") String configId) {
    return configManager.getAsMap(configId);
  }

  /**
   * Get property 
   * @param configId The ID of the configuration
   * @param propertyId The name(key) of the property
   * @return value of the specified property in the provided configId.
   */
  @GET
  @Path("/{configId}/{propertyId}")
  @Produces(MediaType.TEXT_PLAIN)
	@JacksonFeatures(serializationEnable =  { SerializationFeature.INDENT_OUTPUT })
  public String getProperty(@PathParam("configId") String configId,
      @PathParam("propertyId") String propertyId) {
    return configManager.getAsMap(configId).get(propertyId);
  }

  /**
   * Removes the configuration from the list of known configurations. 
   * @param configId The ID of the configuration to delete
   */
  @DELETE
  @Path("/{configId}")
  public void deleteConfig(@PathParam("configId") String configId) {
    configManager.delete(configId);
  }

  /**
   * Create new configuration.
   * @param newConfig 
   * @return The name of the new configuration created
   */
  @POST
  @Path("/create")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.TEXT_PLAIN)
  public Response createConfig(NutchConfig newConfig) {
    if (newConfig == null) {
      return Response.status(400)
          .entity("Nutch configuration cannot be empty!").build();
    }
    try{
      configManager.create(newConfig);
    }catch(Exception e){
      return Response.status(400)
      .entity(e.getMessage()).build();
    }
    return Response.ok(newConfig.getConfigId()).build();
  }
  
  /**
   * Adds/Updates a particular property value in the configuration
   * @param confId Configuration ID whose property needs to be updated. Make sure that the given
   *               confId exists to prevent errors. 
   * @param propertyKey Name of the property
   * @param value Value as a simple text 
   * @return Success code
   */
  @PUT
  @Path("/{configId}/{propertyId}")
  @Consumes(MediaType.TEXT_PLAIN)
  public Response updateProperty(@PathParam("configId")String confId, 
      @PathParam("propertyId")String propertyKey, String value) {
    try{
    configManager.setProperty(confId, propertyKey, value);
    }catch(Exception e) {
      return Response.status(400).entity(e.getMessage()).build();
    }
    return Response.ok().build();
  }
}
