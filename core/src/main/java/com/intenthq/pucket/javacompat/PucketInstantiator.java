package com.intenthq.pucket.javacompat;

import com.intenthq.pucket.Pucket;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public interface PucketInstantiator<V> {
  <T extends V> Pucket<T> newInstance(Path path, FileSystem fs, String descriptor) throws RuntimeException, IOException;
}
