package org.apache.nutch.api;

import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.util.Iterator;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.nutch.util.TableUtil;
import org.restlet.data.Form;
import org.restlet.data.MediaType;
import org.restlet.ext.jackson.JacksonConverter;
import org.restlet.representation.OutputRepresentation;
import org.restlet.representation.Representation;
import org.restlet.representation.Variant;
import org.restlet.resource.ResourceException;
import org.restlet.resource.ServerResource;

public class DbResource extends ServerResource {
  public static final String PATH = "db";
  public static final String DESCR = "DB data streaming";

  static JacksonConverter cnv = new JacksonConverter();
  WeakHashMap<String,DbReader> readers = new WeakHashMap<String,DbReader>();
  
  @Override
  protected void doInit() throws ResourceException {
    super.doInit();
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
  }

  @Override
  protected Representation get(final Variant variant) throws ResourceException {
    String startKey = null;
    String endKey = null;
    String rStartKey = null; // reversed keys
    String rEndKey = null;
    String[] fields = null;
    String batchId = null;
    String confId = ConfResource.DEFAULT_CONF;
    Form form = getQuery();
    if (form != null) {
      startKey = form.getFirstValue("start");
      endKey = form.getFirstValue("end");
      rStartKey = form.getFirstValue("rstart");
      rEndKey = form.getFirstValue("rend");
      if (rStartKey != null || rEndKey != null) {
        startKey = rStartKey;
        endKey = rEndKey;
      } else {
        if (startKey != null) {
          try {
            startKey = TableUtil.reverseUrl(startKey);
          } catch (MalformedURLException e) { /*ignore */};
        }
        if (endKey != null) {
          try {
            endKey = TableUtil.reverseUrl(endKey);
          } catch (MalformedURLException e) { /*ignore */};
        }
      }
      batchId = form.getFirstValue("batch");
      String flds = form.getFirstValue("fields");
      if (flds != null && flds.trim().length() > 0) {
        flds = flds.replaceAll("\\s+", "");
        fields = flds.split(",");
      }
    }
    DbReader reader;
    synchronized (readers) {
      reader = readers.get(confId);
      if (reader == null) {
        reader = new DbReader(NutchApp.confMgr.get(confId), null);
        readers.put(confId, reader);
      }
    }
    Representation res = new DbRepresentation(this, variant, reader, fields,
        startKey, endKey, batchId);
    return res;
  }
  
  private static class DbRepresentation extends OutputRepresentation {
    private DbReader r;
    private Variant variant;
    private String[] fields;
    private String startKey, endKey, batchId;
    private DbResource resource;
    
    public DbRepresentation(DbResource resource, Variant variant, DbReader reader,
        String[] fields, String startKey, String endKey, String batchId) {
      super(variant.getMediaType());
      this.resource = resource;
      this.r = reader;
      this.variant = variant;
      this.fields = fields;
      this.startKey = startKey;
      this.endKey = endKey;
      this.batchId = batchId;
    }

    @Override
    public void write(OutputStream out) throws IOException {
      try {
        out.write('[');
        Iterator<Map<String,Object>> it = r.iterator(fields, startKey, endKey, batchId);
        boolean first = true;
        while (it.hasNext()) {
          if (!first) {
            out.write(',');
          } else {
            first = false;
          }
          Map<String,Object> item = it.next();
          Representation repr = cnv.toRepresentation(item, variant, resource);
          repr.write(out);
          out.flush();
          repr.release();
        }
        out.write(']');
      } catch (Exception e) {
        throw new IOException("DbReader.iterator failed", e);
      }
    }
  }
}
