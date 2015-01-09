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
package org.apache.nutch.indexer.tld;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.storage.WebPage;

import org.junit.Test;

/**
 * JUnit test case which populates a HashMap with URL's and top level domain
 * qualifiers as key's and value's respectively. We assert that each value entry
 * in the HashMap equals the expect field value for the document after being
 * filtered.
 * 
 */

public class TestTLDIndexingFilter {

  @Test
  public void testBasicFields() throws Exception {
    Map<String, String> urls = new HashMap<String, String>();

    urls.put("http://www.a.com", "com");
    urls.put("http://www.b.aero", "aero");
    urls.put("http://www.c.coop", "coop");
    urls.put("http://d.biz", "biz");
    urls.put("http://www.e.cat", "cat");
    urls.put("http://www.lib.f.edu", "edu");
    urls.put("http://www.g.gov", "gov");
    urls.put("http://www.h.int", "int");
    urls.put("http://www.i.mil", "mil");
    urls.put("http://j.net", "net");
    urls.put("http://www.k.org", "org");
    urls.put("http://www.l.pro", "pro");
    urls.put("http://www.m.int", "int");
    urls.put("http://www.n.nato", "nato");
    urls.put("http://www.o.bitnet", "bitnet");
    urls.put("http://www.p.ubc.ca", "ca");
    urls.put("http://www.q.ubc.an", "an");
    urls.put("http://www.r.ubc.ch", "ch");
    urls.put("http://www.s.ubc.eu", "eu");
    urls.put("http://www.t.ubc.kp", "kp");
    urls.put("http://www.u.ubc.nz", "nz");
    urls.put("http://www.v.ubc.tr", "tr");
    urls.put("http://www.w.ubc.yu", "yu");
    urls.put("http://www.x.id.us", "id.us");
    urls.put("http://www.w.gu.us", "gu.us");
    urls.put("http://www.z.vi.us", "vi.us");
    urls.put("http://www.a.co.uk/", "co.uk");
    urls.put("http://www.b.gov.uk/", "gov.uk");
    urls.put("http://www.c.nic.uk/", "nic.uk");
    urls.put("http://www.d.govt.uk/", "govt.uk");
    urls.put("http://www.e.orgn.uk/", "orgn.uk");
    urls.put("http://www.f.com.tr/", "com.tr");
    urls.put("http://www.g.web.tr/", "web.tr");
    urls.put("http://www.h.tel.tr/", "tel.tr");
    urls.put("http://i.nom.ad", "nom.ad");
    urls.put("http://j.tp", "tp");
    urls.put("http://k.e164.arpa", "e164.arpa");
    urls.put("http://l.ip6.arpa", "ip6.arpa");
    urls.put("http://m.act.edu.au", "act.edu.au");
    urls.put("http://n.한글.kr", "한글.kr");
    urls.put("http://p.åfjord.no", "åfjord.no");
    urls.put("http://q.åmot.no", "åmot.no");
    urls.put("http://r.lærdal.no", "lærdal.no");
    urls.put("http://s.組織.tw", "組織.tw");
    urls.put("http://t.gs.aa.no", "gs.aa.no");
    urls.put("http://u.gs.oslo.no/", "gs.oslo.no");
    urls.put("https://v.florø.no/", "florø.no");
    urls.put("ftp://w.info.nf/", "info.nf");
    urls.put("file://x.aa.no", "aa.no");

    WebPage page = WebPage.newBuilder().build();

    TLDIndexingFilter filter = new TLDIndexingFilter();
    assertNotNull(filter);

    for (Entry<String, String> entry : urls.entrySet()) {
      NutchDocument doc = new NutchDocument();
      assertNotNull(doc);
      String url = entry.getKey();
      try {
        filter.filter(doc, url, page);
      } catch (Exception e) {
        e.printStackTrace();
        fail(e.getMessage());
      }

      assertEquals(doc.getFieldValue("tld"), entry.getValue());
    }

  }

}
