package org.apache.nutch.util;


import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.DefaultHandler;
import org.mortbay.jetty.handler.HandlerList;
import org.mortbay.jetty.handler.ResourceHandler;

public class HelloHandler {

    public static void main( String[] args ) throws Exception
    {
        Server webServer = new org.mortbay.jetty.Server(55000);
        ResourceHandler handler = new ResourceHandler();
        HandlerList handlers = new HandlerList();
        handler.setResourceBase("build/test/data/fetch-test-site");
        handlers.setHandlers(new Handler[] { handler, new DefaultHandler() });
        webServer.setHandler(handlers);
        webServer.start();
    }
}