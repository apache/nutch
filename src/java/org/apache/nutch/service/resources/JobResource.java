package org.apache.nutch.service.resources;

import java.util.Collection;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.apache.nutch.service.model.response.JobConfig;
import org.apache.nutch.service.model.response.JobInfo;
import org.apache.nutch.service.model.response.JobInfo.State;

@Path(value = "/job")
public class JobResource extends AbstractResource {

  @GET
  @Path(value = "/")
  public Collection<JobInfo> getJobs(@QueryParam("crawlId") String crawlId) {
    return jobManager.list(crawlId, State.ANY);
  }

  @GET
  @Path(value = "/{id}")
  public JobInfo getInfo(@PathParam("id") String id,
      @QueryParam("crawlId") String crawlId) {
    return jobManager.get(crawlId, id);
  }

  @GET
  @Path(value = "/{id}/stop")
  public boolean stop(@PathParam("id") String id,
      @QueryParam("crawlId") String crawlId) {
    return jobManager.stop(crawlId, id);
  }

  @GET
  @Path(value = "/{id}/abort")
  public boolean abort(@PathParam("id") String id,
      @QueryParam("crawlId") String crawlId) {
    return jobManager.abort(crawlId, id);
  }

  @POST
  @Path(value = "/create")
  @Consumes(MediaType.APPLICATION_JSON)
  public String create(JobConfig config) {
    if (config == null) {
      throwBadRequestException("Job configuration is required!");
    }
    return jobManager.create(config);
  }
}
