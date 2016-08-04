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
package org.apache.nutch.indexwriter.elasticrest;

public interface ElasticRestConstants {
    public static final String ELASTIC_PREFIX = "elastic.rest.";

    public static final String HOST = ELASTIC_PREFIX + "host";
    public static final String PORT = ELASTIC_PREFIX + "port";
    public static final String INDEX = ELASTIC_PREFIX + "index";
    public static final String MAX_BULK_DOCS = ELASTIC_PREFIX + "max.bulk.docs";
    public static final String MAX_BULK_LENGTH = ELASTIC_PREFIX + "max.bulk.size";

    public static final String USER = ELASTIC_PREFIX + "user";
    public static final String PASSWORD = ELASTIC_PREFIX + "password";
    public static final String TYPE = ELASTIC_PREFIX + "type";
    public static final String HTTPS = ELASTIC_PREFIX + "https";
    public static final String HOSTNAME_TRUST = ELASTIC_PREFIX + "trustallhostnames";
}