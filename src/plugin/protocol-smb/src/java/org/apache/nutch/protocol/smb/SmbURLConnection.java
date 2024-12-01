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
package org.apache.nutch.protocol.smb;

import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;

public class SmbURLConnection extends URLConnection {

    private String schema;
    private String host;
    private int port;
    private String share;
    private String path;

    public SmbURLConnection(URL url) {
        super(url);

        try {
            String u = java.net.URLDecoder.decode(url.toString(), StandardCharsets.UTF_8.name());
            String[] parts = u.split("://");
            schema = parts[0];
            u = parts[1];

            parts = u.split("[:/]", 2);
            host = parts[0];
            u = parts[1]; // we have share and path now

            parts = u.split("/", 2);
            share = parts[0];

            path = "/" + parts[1];
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException("could not decypher given url", e);
        }
    }

    public String getSchema() {
        return schema;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getShare() {
        return share;
    }

    public String getPath() {
        return path;
    }

    public void connect() {

    }
}