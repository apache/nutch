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

    public SmbURLConnection(URL url) throws UnsupportedEncodingException {
        super(url);

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