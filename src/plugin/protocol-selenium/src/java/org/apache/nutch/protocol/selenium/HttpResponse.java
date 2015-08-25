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

package org.apache.nutch.protocol.selenium;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.SpellCheckedMetadata;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.storage.WebPage;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;

// import org.apache.nutch.crawl.CrawlDatum;

/* Most of this code was borrowed from protocol-htmlunit; which in turn borrowed it from protocol-httpclient */

public class HttpResponse implements Response {

    private Http http;
    private URL url;
    private String orig;
    private String base;
    private byte[] content;
    private int code;
    private Metadata headers = new SpellCheckedMetadata();

    /**
     * The nutch configuration
     */
    private Configuration conf = null;

    public HttpResponse(Http http, URL url, WebPage page, Configuration conf) throws UnsupportedEncodingException {

        this.conf = conf;
        this.http = http;
        this.url = url;
        this.orig = url.toString();
        this.base = url.toString();

        WebDriver driver = new FirefoxDriver();
        try {
            driver.get(url.toString());
            // Wait for the page to load, timeout after 3 seconds
            new WebDriverWait(driver, 3);

            String innerHtml = driver.findElement(By.tagName("body")).getAttribute("innerHTML");
            code = 200;
            content = innerHtml.getBytes("UTF-8");
        } finally {
            driver.close();
        }
    }

    @Override
    public URL getUrl() {
        return url;
    }

    @Override
    public int getCode() {
        return code;
    }

    @Override
    public String getHeader(String name) {
        return headers.get(name);
    }

    @Override
    public Metadata getHeaders() {
        return headers;
    }

    @Override
    public byte[] getContent() {
        return content;
    }
}
