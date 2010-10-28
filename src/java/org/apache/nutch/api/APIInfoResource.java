package org.apache.nutch.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

public class APIInfoResource extends ServerResource {
  private static final List<String[]> info = new ArrayList<String[]>();
  
  static {
    info.add(new String[]{ConfResource.PATH, ConfResource.DESCR});
    info.add(new String[]{JobResource.PATH, JobResource.DESCR});
  }

  @Get("json")
  public List<String[]> retrieve() throws IOException {
    return info;
  }
}
