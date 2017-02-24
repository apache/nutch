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
package org.apache.nutch.service.impl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.ws.rs.WebApplicationException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.scoring.webgraph.LinkDatum;
import org.apache.nutch.service.NutchReader;

public class LinkReader implements NutchReader{

  @Override
  public List read(String path) throws FileNotFoundException {
    List<HashMap> rows=new ArrayList<HashMap>();
    Path file = new Path(path);
    SequenceFile.Reader reader;
    try{
      reader = new SequenceFile.Reader(conf, Reader.file(file));
      Writable key = (Writable)
          ReflectionUtils.newInstance(reader.getKeyClass(), conf);
      LinkDatum value = new LinkDatum();

      while(reader.next(key, value)) {
        try {
          HashMap<String, String> t_row = getLinksRow(key,value);
          rows.add(t_row);
        }
        catch (Exception e) {
        }
      }
      reader.close();

    }catch(FileNotFoundException fne){ 
      throw new FileNotFoundException();

    }catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      LOG.error("Error occurred while reading file {} : ", file, StringUtils.stringifyException(e));
      throw new WebApplicationException();
    } 

    return rows;
  }

  @Override
  public List head(String path, int nrows) throws FileNotFoundException {
    List<HashMap> rows=new ArrayList<HashMap>();
    Path file = new Path(path);
    SequenceFile.Reader reader;
    try{
      reader = new SequenceFile.Reader(conf, Reader.file(file));
      Writable key = (Writable)
          ReflectionUtils.newInstance(reader.getKeyClass(), conf);
      LinkDatum value = new LinkDatum();
      int i = 0;
      while(reader.next(key, value) && i<nrows) {

        HashMap<String, String> t_row = getLinksRow(key,value);
        rows.add(t_row);

        i++;
      }
      reader.close();

    }catch(FileNotFoundException fne){ 
      throw new FileNotFoundException();

    }catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      LOG.error("Error occurred while reading file {} : ", file, StringUtils.stringifyException(e));
      throw new WebApplicationException();
    } 

    return rows;
  }

  @Override
  public List slice(String path, int start, int end)
      throws FileNotFoundException {
    List<HashMap> rows=new ArrayList<HashMap>();
    Path file = new Path(path);
    SequenceFile.Reader reader;
    try{
      reader = new SequenceFile.Reader(conf, Reader.file(file));
      Writable key = (Writable)
          ReflectionUtils.newInstance(reader.getKeyClass(), conf);
      LinkDatum value = new LinkDatum();
      int i = 0;

      for(;i<start && reader.next(key, value);i++){} // increment to read start position
      while(reader.next(key, value) && i<end) {
        HashMap<String, String> t_row = getLinksRow(key,value);
        rows.add(t_row);

        i++;
      }
      reader.close();

    }catch(FileNotFoundException fne){ 
      throw new FileNotFoundException();

    }catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      LOG.error("Error occurred while reading file {} : ", file, StringUtils.stringifyException(e));
      throw new WebApplicationException();
    } 

    return rows;
  }

  @Override
  public int count(String path) throws FileNotFoundException {
    Path file = new Path(path);
    SequenceFile.Reader reader;
    int i = 0;
    try {
      reader = new SequenceFile.Reader(conf, Reader.file(file));
      Writable key = (Writable)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
      Writable value = (Writable)ReflectionUtils.newInstance(reader.getValueClass(), conf);

      while(reader.next(key, value)) {
        i++;
      }
      reader.close();
    } catch(FileNotFoundException fne){ 
      throw new FileNotFoundException();
    }catch (IOException e) {
      // TODO Auto-generated catch block
      LOG.error("Error occurred while reading file {} : ", file, StringUtils.stringifyException(e));
      throw new WebApplicationException();
    } 
    return i;
  }

  private HashMap<String, String> getLinksRow(Writable key, LinkDatum value) {
    HashMap<String, String> t_row = new HashMap<String, String>();
    t_row.put("key_url", key.toString());
    t_row.put("url", value.getUrl());
    t_row.put("anchor", value.getAnchor());
    t_row.put("score", String.valueOf(value.getScore()));
    t_row.put("timestamp", String.valueOf(value.getTimestamp()));
    t_row.put("linktype", String.valueOf(value.getLinkType()));

    return t_row;
  }
}
