package com.intenthq.pucket.javacompat.avro;

import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.intenthq.pucket.Pucket;
import com.intenthq.pucket.avro.AvroPucket;
import com.intenthq.pucket.avro.AvroPucketDescriptor;
import com.intenthq.pucket.javacompat.PucketInstantiator;
import com.intenthq.pucket.util.ExceptionUtil;

import java.io.IOException;

public class AvroPucketInstantiator implements PucketInstantiator<IndexedRecord> {
  @Override
  public <T extends IndexedRecord> Pucket<T> newInstance(final Path path, final FileSystem fs, final String descriptor)
      throws RuntimeException, IOException {
    return ExceptionUtil.doThrow(
        AvroPucket.findOrCreate(path, fs, ExceptionUtil.doThrow(
            AvroPucketDescriptor.<T>apply(descriptor))
        )
    );
  }
}
