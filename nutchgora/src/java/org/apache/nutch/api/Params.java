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
package org.apache.nutch.api;

public interface Params {
  
  public static final String CONF_ID = "conf";
  public static final String PROP_NAME = "prop";
  public static final String PROP_VALUE = "value";
  public static final String PROPS = "props";
  public static final String CRAWL_ID = "crawl";
  public static final String JOB_ID = "job";
  public static final String JOB_TYPE = "type";
  public static final String ARGS = "args";
  public static final String CMD = "cmd";
  public static final String FORCE = "force";
  
  
  public static final String JOB_CMD_STOP = "stop";
  public static final String JOB_CMD_ABORT = "abort";
  public static final String JOB_CMD_GET = "get";
}
