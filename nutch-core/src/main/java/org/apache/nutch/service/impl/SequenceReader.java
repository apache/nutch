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
import java.util.List;

import javax.ws.rs.WebApplicationException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.service.NutchReader;

/**
 * Enables reading a sequence file and methods provide different 
 * ways to read the file. 
 * @author Sujen Shah
 *
 */
public class SequenceReader implements NutchReader {

  @Override
  public List<List<String>> read(String path) throws FileNotFoundException {
    // TODO Auto-generated method stub
    List<List<String>> rows=new ArrayList<List<String>>();
    Path file = new Path(path);
    SequenceFile.Reader reader;
    try {
      reader = new SequenceFile.Reader(conf, Reader.file(file));
      Writable key = 
          (Writable)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
      Writable value = 
          (Writable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
      
      while(reader.next(key, value)) {
        List<String> row =new ArrayList<String>();
        row.add(key.toString());
        row.add(value.toString());
        rows.add(row);
      }
      reader.close();
    }catch(FileNotFoundException fne){ 
      throw new FileNotFoundException();
    }catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      LOG.error("Error occurred while reading file {} : ", file, 
          StringUtils.stringifyException(e));
      throw new WebApplicationException();
    } 
    return rows;
  }

  @Override
  public List<List<String>> head(String path, int nrows) 
      throws FileNotFoundException {
    // TODO Auto-generated method stub
    
    List<List<String>> rows=new ArrayList<List<String>>();
    Path file = new Path(path);
    SequenceFile.Reader reader;
    try {
      
      reader = new SequenceFile.Reader(conf, Reader.file(file));
      Writable key = 
          (Writable)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
      Writable value = 
          (Writable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
      int i = 0;
      while(reader.next(key, value) && i<nrows) {
        List<String> row =new ArrayList<String>();
        row.add(key.toString());
        row.add(value.toString());
        rows.add(row);
        i++;
      }
      reader.close();
    } catch(FileNotFoundException fne){ 
      throw new FileNotFoundException();
    }catch (IOException e) {
      // TODO Auto-generated catch block
      LOG.error("Error occurred while reading file {} : ", file, 
          StringUtils.stringifyException(e));
      throw new WebApplicationException();
    } 
    return rows;
  }

  @Override
  public List<List<String>> slice(String path, int start, int end) 
      throws FileNotFoundException {
    List<List<String>> rows=new ArrayList<List<String>>();
    Path file = new Path(path);
    SequenceFile.Reader reader;
    try {
      
      reader = new SequenceFile.Reader(conf, Reader.file(file));
      Writable key = 
          (Writable)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
      Writable value = 
          (Writable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
      int i = 0;
      
      for(;i<start && reader.next(key, value);i++){} // increment to read start position
      while(reader.next(key, value) && i<end) {
        List<String> row =new ArrayList<String>();
        row.add(key.toString());
        row.add(value.toString());
        rows.add(row);
        i++;
      }
      reader.close();
    } catch(FileNotFoundException fne){ 
      throw new FileNotFoundException();
    }catch (IOException e) {
      // TODO Auto-generated catch block
      LOG.error("Error occurred while reading file {} : ", file, 
          StringUtils.stringifyException(e));
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
      Writable key = 
          (Writable)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
      Writable value = 
          (Writable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
     
      while(reader.next(key, value)) {
        i++;
      }
      reader.close();
    } catch(FileNotFoundException fne){ 
      throw new FileNotFoundException();
    }catch (IOException e) {
      // TODO Auto-generated catch block
      LOG.error("Error occurred while reading file {} : ", file, 
          StringUtils.stringifyException(e));
      throw new WebApplicationException();
    } 
    return i;
  }

}
