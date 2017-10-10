package org.apache.nutch.plugin;

import java.lang.ref.WeakReference;
import java.net.URL;
import java.net.URLStreamHandler;
import java.util.ArrayList;

import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This URLStreamHandlerFactory knows about all the plugins
 * in use and thus can create the correct URLStreamHandler
 * even if it comes from a plugin classpath.
 * As the JVM allows only one instance of URLStreamHandlerFactory
 * to be registered, this class implements a singleton pattern.
 * @author Hiran Chaudhuri
 *
 */
public class URLStreamHandlerFactory
    implements java.net.URLStreamHandlerFactory {
  
  protected static final Logger LOG = LoggerFactory
      .getLogger(URLStreamHandlerFactory.class);
  
  /** The singleton instance. */
  private static URLStreamHandlerFactory instance;
  
  /** Here we register all PluginRepositories.
   * In this class we do not know why several instances of PluginRepository
   * are kept, nor do we know how long they will be used. To prevent
   * a memory leak, this class must not keep references to PluginRepository
   * but use WeakReference which allows PluginRepository to still be
   * garbage collected. The prize is we need to clean the list for
   * outdated references which is done in the {@link removeInvalidRefs} method.  
   */
  private ArrayList<WeakReference<PluginRepository>> prs;
  
  static {
    instance = new URLStreamHandlerFactory();
    URL.setURLStreamHandlerFactory(instance);
    LOG.info("Registered URLStreamHandlerFactory with the JVM.");
  }
  
  private URLStreamHandlerFactory() {
    LOG.debug("URLStreamHandlerFactory()");
    prs = new ArrayList<>();
  }

  /** Return the singleton instance of this class. */
  public static URLStreamHandlerFactory getInstance() {
    LOG.debug("getInstance()");
    return instance;
  }
  
  /** Use this method once a new PluginRepository was created to register it.
   * 
   * @param pr The PluginRepository to be registered.
   */
  public void registerPluginRepository(PluginRepository pr) {
    LOG.debug("registerPluginRepository(...)");
    prs.add(new WeakReference<PluginRepository>(pr));
    
    removeInvalidRefs();
  }

  @Override
  public URLStreamHandler createURLStreamHandler(String protocol) {
    LOG.debug("createURLStreamHandler({})", protocol);
    
    removeInvalidRefs();
    
    // find the 'correct' PluginRepository. For now we simply take the first.
    // then ask it to return the URLStreamHandler

    for(WeakReference<PluginRepository> ref: prs) {
      PluginRepository pr = ref.get();
      if(pr != null) {
        // found PluginRepository. Let's get the URLStreamHandler...
        return pr.createURLStreamHandler(protocol);
      }
    }
    return null;
  }

  /** Maintains the list of PluginRepositories by
   * removing the references whose referents have been
   * garbage collected meanwhile.
   */
  private void removeInvalidRefs() {
    LOG.debug("removeInvalidRefs()");
    ArrayList<WeakReference<PluginRepository>> copy = new ArrayList<>(prs);
    for(WeakReference<PluginRepository> ref: copy) {
      if(ref.get() == null) {
        prs.remove(ref);
      }
    }
    LOG.debug("removed {} references, remaining {}", copy.size()-prs.size(), prs.size());
  }
}
