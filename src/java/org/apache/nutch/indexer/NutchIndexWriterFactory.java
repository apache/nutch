package org.apache.nutch.indexer;

import org.apache.hadoop.conf.Configuration;

public class NutchIndexWriterFactory {
  @SuppressWarnings("unchecked")
  public static NutchIndexWriter[] getNutchIndexWriters(Configuration conf) {
    final String[] classes = conf.getStrings("indexer.writer.classes");
    final NutchIndexWriter[] writers = new NutchIndexWriter[classes.length];
    for (int i = 0; i < classes.length; i++) {
      final String clazz = classes[i];
      try {
        final Class<NutchIndexWriter> implClass =
          (Class<NutchIndexWriter>) Class.forName(clazz);
        writers[i] = implClass.newInstance();
      } catch (final Exception e) {
        throw new RuntimeException("Couldn't create " + clazz, e);
      }
    }
    return writers;
  }

  public static void addClassToConf(Configuration conf,
                                    Class<? extends NutchIndexWriter> clazz) {
    final String classes = conf.get("indexer.writer.classes");
    final String newClass = clazz.getName();

    if (classes == null) {
      conf.set("indexer.writer.classes", newClass);
    } else {
      conf.set("indexer.writer.classes", classes + "," + newClass);
    }

  }

}
