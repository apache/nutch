/* Copyright (c) 2004 The Nutch Organization.  All rights reserved.   */
/* Use subject to the conditions in http://www.nutch.org/LICENSE.txt. */

package org.apache.nutch.protocol.httpclient;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.nutch.protocol.ContentProperties;
import org.apache.nutch.util.LogFormatter;
import org.apache.nutch.util.NutchConf;

/**
 * Provides the Http protocol implementation
 * with the ability to authenticate when prompted.  The goal is to provide 
 * multiple authentication types but for now just the {@link HttpBasicAuthentication} authentication 
 * type is provided.
 * 
 * @see HttpBasicAuthentication
 * @see Http
 * @see HttpResponse
 * 
 * @author Matt Tencati
 */
public class HttpAuthenticationFactory {
    /** The HTTP Authentication (WWW-Authenticate) header which is returned 
     * by a webserver requiring authentication.
     */
    public static final String AUTH_HEADER = "WWW-Authenticate";
	
	public static final Logger LOG =
		LogFormatter.getLogger("net.nutch.protocol.http.HttpAuthenticationFactory");

	static {
		if (NutchConf.get().getBoolean("http.auth.verbose", false))
			LOG.setLevel(Level.FINE);
	}
	  
    private static Map auths = new TreeMap(); 
    
    private HttpAuthenticationFactory() { }
    
    public static HttpAuthentication findAuthentication(ContentProperties header) {
        if (header == null) return null;
        
    	try {
			Collection challenge = null;
			if (header instanceof ContentProperties) {
				Object o = header.get(AUTH_HEADER);
				if (o instanceof Collection) {
					challenge = (Collection) o;
				} else {
					challenge = new ArrayList();
					challenge.add(o.toString());
				}
			} else {
				String challengeString = header.getProperty(AUTH_HEADER); 
				if (challengeString != null) {
					challenge = new ArrayList();
					challenge.add(challengeString);
				}
			}
			if (challenge == null) {
				LOG.fine("Authentication challenge is null");
				return null;
			}
			
			Iterator i = challenge.iterator();
			HttpAuthentication auth = null;
			while (i.hasNext() && auth == null) {
				String challengeString = (String)i.next();
				if (challengeString.equals("NTLM")) {
				   challengeString="Basic realm=techweb";
		                  }
		                
		                LOG.fine("Checking challengeString=" + challengeString);
				auth = HttpBasicAuthentication.getAuthentication(challengeString);
				if (auth != null) return auth;
				
				//TODO Add additional Authentication lookups here
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
        return null;
    }
}
