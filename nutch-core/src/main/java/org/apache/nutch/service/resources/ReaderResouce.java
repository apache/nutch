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

import java.util.HashMap;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.nutch.service.NutchReader;
import org.apache.nutch.service.impl.LinkReader;
import org.apache.nutch.service.impl.NodeReader;
import org.apache.nutch.service.impl.SequenceReader;
import org.apache.nutch.service.model.request.ReaderConfig;

/**
 * The Reader endpoint enables a user to read sequence files, 
 * nodes and links from the Nutch webgraph.
 * @author Sujen Shah
 *
 */
@Path("/reader")
public class ReaderResouce {

  /**
   * Read a sequence file
   * @param readerConf 
   * @param nrows Number of rows to read. If not specified all rows will be read
   * @param start Specify a starting line number to read the file from
   * @param end The line number to read the file till
   * @param count Boolean value. If true, this endpoint will return the number of lines in the line
   * @return Appropriate HTTP response based on the query
   */
  @Path("/sequence/read")
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response seqRead(ReaderConfig readerConf, 
      @DefaultValue("-1")@QueryParam("nrows") int nrows, 
      @DefaultValue("-1")@QueryParam("start") int start, 
      @QueryParam("end")int end, @QueryParam("count") boolean count) {

    NutchReader reader = new SequenceReader();
    String path = readerConf.getPath();
    return performRead(reader, path, nrows, start, end, count);
  }

  /**
   * Get Link Reader response schema 
   * @return JSON object specifying the schema of the responses returned by the Link Reader
   */
  @Path("/link")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response linkRead() {
    HashMap<String, String> schema = new HashMap<>();
    schema.put("key_url","string");
    schema.put("timestamp", "int");
    schema.put("score","float"); 
    schema.put("anchor","string");
    schema.put("linktype","string");
    schema.put("url","string");
    return Response.ok(schema).type(MediaType.APPLICATION_JSON).build();
  }

  /**
   * Read link object 
   * @param readerConf
   * @param nrows
   * @param start
   * @param end
   * @param count
   * @return
   */
  @Path("/link/read")
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response linkRead(ReaderConfig readerConf, 
      @DefaultValue("-1")@QueryParam("nrows") int nrows, 
      @DefaultValue("-1")@QueryParam("start") int start, 
      @QueryParam("end") int end, @QueryParam("count") boolean count) {

    NutchReader reader = new LinkReader();
    String path = readerConf.getPath();
    return performRead(reader, path, nrows, start, end, count);
  }

  /**
   * Get schema of the Node object
   * @return
   */
  @Path("/node")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response nodeRead() {
    HashMap<String, String> schema = new HashMap<>();
    schema.put("key_url","string");
    schema.put("num_inlinks", "int");
    schema.put("num_outlinks","int");
    schema.put("inlink_score","float"); 
    schema.put("outlink_score","float"); 
    schema.put("metadata","string");
    return Response.ok(schema).type(MediaType.APPLICATION_JSON).build();
  }


  /**
   * Read Node object as stored in the Nutch Webgraph
   * @param readerConf
   * @param nrows
   * @param start
   * @param end
   * @param count
   * @return
   */
  @Path("/node/read")
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response nodeRead(ReaderConfig readerConf, 
      @DefaultValue("-1")@QueryParam("nrows") int nrows, 
      @DefaultValue("-1")@QueryParam("start") int start, 
      @QueryParam("end") int end, @QueryParam("count") boolean count) {

    NutchReader reader = new NodeReader();
    String path = readerConf.getPath();
    return performRead(reader, path, nrows, start, end, count);
  }


  private Response performRead(NutchReader reader, String path, 
      int nrows, int start, int end, boolean count) {
    Object result;
    try{
      if(count){
        result = reader.count(path);
        return Response.ok(result).type(MediaType.TEXT_PLAIN).build();
      }
      else if(start>-1 && end>0) {
        result = reader.slice(path, start, end);
      }
      else if(nrows>-1) {
        result = reader.head(path, nrows);
      }
      else {
        result = reader.read(path);
      }
      return Response.ok(result).type(MediaType.APPLICATION_JSON).build();
    }catch(Exception e){
      return Response.status(Status.BAD_REQUEST).entity("File not found").build();
    }
  }

}
