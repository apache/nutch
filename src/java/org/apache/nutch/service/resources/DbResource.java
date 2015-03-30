package org.apache.nutch.service.resources;


import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.CrawlDbReader;
import org.apache.nutch.service.model.request.DbQuery;

@Path(value = "/db")
public class DbResource extends AbstractResource {

	@SuppressWarnings("resource")
	@POST
	@Path(value = "/")
	@Consumes(MediaType.APPLICATION_JSON)
	public Response readdb(DbQuery dbQuery){
		try {
			Configuration conf = configManager.get(dbQuery.getConfId());
			CrawlDbReader dbr = new CrawlDbReader();
			return Response.ok(dbr.query(dbQuery.getArgs(), conf)).build();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return Response.serverError().entity(e.getMessage()).build();
		}		
	}
}
