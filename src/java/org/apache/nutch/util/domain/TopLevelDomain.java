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

package org.apache.nutch.util.domain;

/**
 * (From wikipedia) A top-level domain (TLD) is the last part of an 
 * Internet domain name; that is, the letters which follow the final 
 * dot of any domain name. For example, in the domain name 
 * <code>www.website.com</code>, the top-level domain is <code>com</code>.
 * @author Enis Soztutar &lt;enis.soz.nutch@gmail.com&gt;
 * @see http://www.iana.org/
 * @see http://en.wikipedia.org/wiki/Top-level_domain
 */
public class TopLevelDomain extends DomainSuffix {

  public enum Type { INFRASTRUCTURE, GENERIC, COUNTRY };
  
  private Type type;
  private String countryName = null;
  
  public TopLevelDomain(String domain, Type type, Status status, float boost){
    super(domain, status, boost);
    this.type = type;
  }

  public TopLevelDomain(String domain, Status status, float boost, String countryName){
    super(domain, status, boost);
    this.type = Type.COUNTRY;
    this.countryName = countryName;
  }
  
  public Type getType() {
    return type;
  }

  /** Returns the country name if TLD is Country Code TLD
   * @return country name or null
   */ 
  public String getCountryName(){
    return countryName;
  }
  
}
