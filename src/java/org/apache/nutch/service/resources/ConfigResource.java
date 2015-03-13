package org.apache.nutch.service.resources;


import java.util.Map;
import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.nutch.service.model.request.NutchConfig;
import org.codehaus.jettison.json.JSONObject;

@Path("/config")
public class ConfigResource extends AbstractResource{
	
	public static final String DEFAULT = "default";

	@GET
	@Path("/")
	public Set<String> getConfigs() {
		return configManager.list();
	}
	
	@GET
	@Path("/{configId}")
	public Map<String, String> getConfig(@PathParam("configId") String configId) {
		return configManager.getAsMap(configId);
	}
	
	@GET
	@Path("/{configId}/{propertyId}")
	public String getProperty(@PathParam("configId") String configId,
			@PathParam("propertyId") String propertyId) {
		return configManager.getAsMap(configId).get(propertyId);
	}

	@DELETE
	@Path("/{configId}")
	public void deleteConfig(@PathParam("configId") String configId) {
		configManager.delete(configId);
	}

	@POST
	@Path("/{configId}")
	@Consumes(MediaType.APPLICATION_JSON)
	public String createConfig(NutchConfig newConfig) {
		if (newConfig == null) {
			throw new WebApplicationException(Response.status(Status.BAD_REQUEST)
					.entity("Nutch configuration cannot be empty!").build());
		}
		return configManager.create(newConfig);
	}
}
