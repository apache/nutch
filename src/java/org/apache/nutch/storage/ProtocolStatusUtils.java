package org.apache.nutch.storage;

import java.net.URL;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.util.Utf8;
import org.apache.nutch.protocol.ProtocolStatusCodes;
import org.apache.nutch.util.TableUtil;

public class ProtocolStatusUtils implements ProtocolStatusCodes {
  // Useful static instances for status codes that don't usually require any
  // additional arguments.
  public static final ProtocolStatus STATUS_SUCCESS = makeStatus(SUCCESS);
  public static final ProtocolStatus STATUS_FAILED = makeStatus(FAILED);
  public static final ProtocolStatus STATUS_GONE = makeStatus(GONE);
  public static final ProtocolStatus STATUS_NOTFOUND = makeStatus(NOTFOUND);
  public static final ProtocolStatus STATUS_RETRY = makeStatus(RETRY);
  public static final ProtocolStatus STATUS_ROBOTS_DENIED = makeStatus(ROBOTS_DENIED);
  public static final ProtocolStatus STATUS_REDIR_EXCEEDED = makeStatus(REDIR_EXCEEDED);
  public static final ProtocolStatus STATUS_NOTFETCHING = makeStatus(NOTFETCHING);
  public static final ProtocolStatus STATUS_NOTMODIFIED = makeStatus(NOTMODIFIED);
  public static final ProtocolStatus STATUS_WOULDBLOCK = makeStatus(WOULDBLOCK);
  public static final ProtocolStatus STATUS_BLOCKED = makeStatus(BLOCKED);

  public static ProtocolStatus makeStatus(int code) {
    ProtocolStatus pstatus = new ProtocolStatus();
    pstatus.setCode(code);
    pstatus.setLastModified(0);
    return pstatus;
  }

  public static ProtocolStatus makeStatus(int code, String message) {
    ProtocolStatus pstatus = makeStatus(code);
    pstatus.addToArgs(new Utf8(message));
    return pstatus;
  }

  public static ProtocolStatus makeStatus(int code, URL url) {
    return makeStatus(code, url.toString());
  }

  public static String getMessage(ProtocolStatus pstatus) {
    GenericArray<Utf8> args = pstatus.getArgs();
    if (args == null || args.size() == 0) {
      return null;
    }
    return TableUtil.toString(args.iterator().next());
  }
}

