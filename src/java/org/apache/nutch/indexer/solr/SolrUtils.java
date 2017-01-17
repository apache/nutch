package org.apache.nutch.indexer.solr;

import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.params.HttpClientParams;
import org.apache.http.params.HttpParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.solr.SolrConstants;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;

public class SolrUtils {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  public static HttpSolrServer getHttpSolrServer(Configuration job)
      throws MalformedURLException {
    DefaultHttpClient client = new DefaultHttpClient();

    // Check for username/password
    if (job.getBoolean(SolrConstants.USE_AUTH, false)) {
      String username = job.get(SolrConstants.USERNAME);

      LOG.info("Authenticating as: " + username);

      AuthScope scope = new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT,
          AuthScope.ANY_REALM, AuthScope.ANY_SCHEME);

      client.getCredentialsProvider().setCredentials(
          scope,
          new UsernamePasswordCredentials(username, job
              .get(SolrConstants.PASSWORD)));

      HttpParams params = client.getParams();
      HttpClientParams.setAuthenticating(params, true);

      client.setParams(params);
    }

    return new HttpSolrServer(job.get(SolrConstants.SERVER_URL), client);
  }

  public static String stripNonCharCodepoints(String input) {
    StringBuilder retval = new StringBuilder();
    char ch;

    for (int i = 0; i < input.length(); i++) {
      ch = input.charAt(i);

      // Strip all non-characters
      // http://unicode.org/cldr/utility/list-unicodeset.jsp?a=[:Noncharacter_Code_Point=True:]
      // and non-printable control characters except tabulator, new line and
      // carriage return
      if (ch % 0x10000 != 0xffff && // 0xffff - 0x10ffff range step 0x10000
          ch % 0x10000 != 0xfffe && // 0xfffe - 0x10fffe range
          (ch <= 0xfdd0 || ch >= 0xfdef) && // 0xfdd0 - 0xfdef
          (ch > 0x1F || ch == 0x9 || ch == 0xa || ch == 0xd)) {

        retval.append(ch);
      }
    }

    return retval.toString();
  }
}