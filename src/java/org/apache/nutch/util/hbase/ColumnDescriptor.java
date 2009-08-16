package org.apache.nutch.util.hbase;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.io.hfile.Compression;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface ColumnDescriptor {
    int versions() default HColumnDescriptor.DEFAULT_VERSIONS;
    Compression.Algorithm compression() default Compression.Algorithm.NONE;
    boolean inMemory() default HColumnDescriptor.DEFAULT_IN_MEMORY;
    boolean blockCacheEnabled() default HColumnDescriptor.DEFAULT_BLOCKCACHE;
    int blockSize() default HColumnDescriptor.DEFAULT_BLOCKSIZE;
    int timeToLive() default HColumnDescriptor.DEFAULT_TTL;
    boolean bloomFilter() default HColumnDescriptor.DEFAULT_BLOOMFILTER;
}
