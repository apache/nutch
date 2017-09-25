package my.foo;

import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

public class Handler extends URLStreamHandler {

  protected URLConnection openConnection(URL u) {
    throw new UnsupportedOperationException("not yet implemented");
  }
}
