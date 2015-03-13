package org.apache.nutch.service.resources;

import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.nutch.service.ConfManager;
import org.apache.nutch.service.JobManager;
import org.apache.nutch.service.NutchServer;

@Produces(MediaType.APPLICATION_JSON)
public abstract class AbstractResource {
	
	protected JobManager jobManager;
	protected ConfManager configManager;
	
	public AbstractResource() {
		configManager = NutchServer.getInstance().getConfManager();
	}
	
	protected void throwBadRequestException(String message) {
		throw new WebApplicationException(Response.status(Status.BAD_REQUEST).entity(message).build());
	}
}
