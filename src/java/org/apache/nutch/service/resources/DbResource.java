package org.apache.nutch.service.resources;


import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.CrawlDbReader;
import org.apache.nutch.service.model.request.DbQuery;

@Path(value = "/db")
public class DbResource extends AbstractResource {

	@POST
	@Path(value = "/crawldb")
	@Consumes(MediaType.APPLICATION_JSON)
	public Object readdb(DbQuery dbQuery){
		Configuration conf = configManager.get(dbQuery.getConfId());
		String type = dbQuery.getType();
		
		if(type.equalsIgnoreCase("stats")){
			return crawlDbStats(conf, dbQuery.getArgs());
		}
		if(type.equalsIgnoreCase("dump")){
			return crawlDbDump(conf, dbQuery.getArgs());
		}
		if(type.equalsIgnoreCase("topN")){
			return crawlDbTopN(conf, dbQuery.getArgs());
		}
		if(type.equalsIgnoreCase("url")){
			return crawlDbUrl(conf, dbQuery.getArgs());
		}
		return null;

	}	

	@SuppressWarnings("resource")
	private Response crawlDbStats(Configuration conf, Map<String, String> args){
		CrawlDbReader dbr = new CrawlDbReader();
		try{
			return Response.ok(dbr.query(args, conf, "stats")).build();
		}catch(Exception e){
			e.printStackTrace();
			return Response.serverError().entity(e.getMessage()).build();
		}
	}
	
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	private Response crawlDbDump(Configuration conf, Map<String, String> args){
		CrawlDbReader dbr = new CrawlDbReader();
		try{
			return Response.ok(dbr.query(args, conf, "dump"), MediaType.APPLICATION_OCTET_STREAM).build();
		}catch(Exception e){
			e.printStackTrace();
			return Response.serverError().entity(e.getMessage()).build();
		}
	}
	
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	private Response crawlDbTopN(Configuration conf, Map<String, String> args) {
		CrawlDbReader dbr = new CrawlDbReader();
		try{
			return Response.ok(dbr.query(args, conf, "topN"), MediaType.APPLICATION_OCTET_STREAM).build();
		}catch(Exception e){
			e.printStackTrace();
			return Response.serverError().entity(e.getMessage()).build();
		}		
	}
	
	private Response crawlDbUrl(Configuration conf, Map<String, String> args){
		CrawlDbReader dbr = new CrawlDbReader();
		try{
			return Response.ok(dbr.query(args, conf, "url")).build();
		}catch(Exception e){
			e.printStackTrace();
			return Response.serverError().entity(e.getMessage()).build();
		}
	}
}
