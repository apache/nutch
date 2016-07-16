package org.apache.nutch.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.net.URL;

public class TestUtils {

  /**
   *
   * @param obj an object whose class's loader should be used
   * @param fileName name of file
   * @return File instance
   * @throws FileNotFoundException when an error occurs or file is not found
   */
  public static File getFile(Object obj, String fileName)
      throws FileNotFoundException {
    try {
      URL resource = obj.getClass().getClassLoader().getResource(fileName);
      if (resource == null) {
        throw new FileNotFoundException(fileName + " not known to classloader of " + obj);
      }
      return new File(resource.toURI());
    } catch (URISyntaxException e) {
      throw new FileNotFoundException(e.getMessage());
    }
  }
}
