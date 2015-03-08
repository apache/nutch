package org.apache.nutch.service;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.ext.Provider;

import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.CommandLine;
import org.apache.cxf.binding.BindingFactoryManager;
import org.apache.cxf.jaxrs.JAXRSBindingFactory;
import org.apache.cxf.jaxrs.JAXRSServerFactoryBean;
import org.apache.cxf.jaxrs.lifecycle.ResourceProvider;
import org.apache.cxf.jaxrs.lifecycle.SingletonResourceProvider;
import org.apache.nutch.service.impl.ConfManagerImpl;
import org.apache.nutch.service.providers.TreeMapProvider;
import org.apache.nutch.service.resources.ConfigResource;
import org.apache.nutch.service.resources.HashMapProvider;
import org.apache.nutch.service.resources.JobResource;
import org.codehaus.jackson.jaxrs.JacksonJaxbJsonProvider;
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
	private ConfManager configManager;
	private JAXRSServerFactoryBean sf; 

	private static NutchServer server;
	
	static {
		server = new NutchServer();
	}
	
	private NutchServer() {
		configManager = new ConfManagerImpl();

		sf = new JAXRSServerFactoryBean();
		String address = "http://" + LOCALHOST + ":" + port;
		sf.setAddress(address);
		BindingFactoryManager manager = sf.getBus().getExtension(BindingFactoryManager.class);
		JAXRSBindingFactory factory = new JAXRSBindingFactory();
		factory.setBus(sf.getBus());
		manager.registerBindingFactory(JAXRSBindingFactory.JAXRS_BINDING_ID, factory);
		sf.setResourceClasses(getClasses());
		sf.setResourceProviders(getResourceProviders());
		sf.setProvider(new JacksonJaxbJsonProvider());
		
		
	}

	public static NutchServer getInstance() {
		return server;
	}
	
	private static void startServer() {
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
		LOG.info("Started Nutch Server on port {} at {}", port, started);
	}

	public List<Class<?>> getClasses() {
		List<Class<?>> resources = new ArrayList<Class<?>>();
		resources.add(JobResource.class);
		resources.add(ConfigResource.class);
		return resources;
	}
	
	public List<ResourceProvider> getResourceProviders() {
		List<ResourceProvider> resourceProviders = new ArrayList<ResourceProvider>();
		resourceProviders.add(new SingletonResourceProvider(getConfManager()));
		
		return resourceProviders;
	}
	
	public ConfManager getConfManager() {
		return configManager;
	}
	
	public static void main(String[] args) throws ParseException {
		CommandLineParser parser = new PosixParser();
		Options options = createOptions();
		CommandLine commandLine = parser.parse(options, args);
		if (commandLine.hasOption(CMD_PORT)) {
			port = Integer.parseInt(commandLine.getOptionValue(CMD_PORT));
		}
		startServer();
	}

	private static Options createOptions() {
		Options options = new Options();
		OptionBuilder.withArgName("port");
		OptionBuilder.hasOptionalArg();
		OptionBuilder.withDescription("The port to run the Nutch Server. Default port 8081");
		options.addOption(OptionBuilder.create(CMD_PORT));
		return options;
	}

}
