package org.apache.nutch.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.cxf.binding.BindingFactoryManager;
import org.apache.cxf.jaxrs.JAXRSBindingFactory;
import org.apache.cxf.jaxrs.JAXRSServerFactoryBean;
import org.apache.nutch.service.resources.ConfigResource;
import org.apache.nutch.service.resources.JobResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NutchServer {

	  private static final Logger LOG = LoggerFactory.getLogger(NutchServer.class);

	  private static final String LOCALHOST = "localhost";
	  private static final String DEFAULT_LOG_LEVEL = "INFO";
	  private static final Integer DEFAULT_PORT = 8081;
	  private static final int JOB_CAPACITY = 100;

	  private static String logLevel = DEFAULT_LOG_LEVEL;
	  private static Integer port = DEFAULT_PORT;

	  private static final String CMD_HELP = "help";
	  private static final String CMD_STOP = "stop";
	  private static final String CMD_PORT = "port";
	  private static final String CMD_LOG_LEVEL = "log";
	  
	  private long started;
	  private boolean running;
	  private JAXRSServerFactoryBean sf; 
	  
	  public NutchServer() {
		  sf = new JAXRSServerFactoryBean();
		  String address = "http://" + LOCALHOST + ":" + port;
		  sf.setAddress(address);
		  BindingFactoryManager manager = sf.getBus().getExtension(BindingFactoryManager.class);
		  JAXRSBindingFactory factory = new JAXRSBindingFactory();
		  factory.setBus(sf.getBus());
		  manager.registerBindingFactory(JAXRSBindingFactory.JAXRS_BINDING_ID, factory);
		  sf.setResourceClasses(getClasses());
	  }
	  
	  private static void startServer() {
		  NutchServer server = new NutchServer();
		  server.start();
	  }
	  
	  private void start() {
		  LOG.info("Starting NutchServer on port: {}  ...",port);
		  try{
			  sf.create();
		  }catch(Exception e){
			  throw new IllegalStateException("Server could not be started", e);
		  }
		  
		  started = System.currentTimeMillis();
		  running = true;
		  LOG.info("Started NutchServer on port {} at {}", port, started);
	  }
	  
	  public List<Class<?>> getClasses() {
		  List<Class<?>> resources = new ArrayList<Class<?>>();
		  resources.add(JobResource.class);
		  resources.add(ConfigResource.class);
		  return resources;
	  }
	  
	  public static void main(String[] args) {
		  
		startServer();
	}
}
