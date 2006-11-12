/*
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
package org.apache.nutch.keymatch;

import java.util.List;
import java.util.Map;

/**
 * <p>Implementation of KeyMatchFilter that simply
 * crops the count of matches to defined level or
 * by default of 3.</p>
 * 
 * <p>The number of results returned is controlled
 * with context parameter under key "count"</p>
 * 
 * @author Sami Siren
 */
public class CountFilter extends AbstractFilter {

  public static final String KEY_COUNT="count";
  public static final int DEFAULT_COUNT=3; 
  
  public KeyMatch[] filter(List matches, Map context) {
    int count=DEFAULT_COUNT;
    
    try{
      count=Integer.parseInt((String)context.get(KEY_COUNT));
    } catch (Exception e){
      //ignore
    }
    
    if(matches.size()>count) {
      return super.filter(matches.subList(0,count), context);
    } else {
      return super.filter(matches, context);
    }
  }
}
