package com.intenthq.pucket.javacompat.thrift;

import com.intenthq.pucket.Pucket;
import com.intenthq.pucket.javacompat.PucketInstantiator;
import com.intenthq.pucket.thrift.ThriftPucket;
import com.intenthq.pucket.thrift.ThriftPucketDescriptor;
import com.intenthq.pucket.util.ExceptionUtil;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;

import java.io.IOException;

public class ThriftPucketInstantiator implements PucketInstantiator<TBase<? extends TBase<?, ?>, ? extends TFieldIdEnum>> {
  @Override
  public <T extends TBase<? extends TBase<?, ?>, ? extends TFieldIdEnum>> Pucket<T> newInstance(final Path path,
                                                                                                final FileSystem fs,
                                                                                                final String descriptor)
      throws RuntimeException, IOException {
    return ExceptionUtil.doThrow(
        ThriftPucket.findOrCreate(path,
                                  fs,
                                  ExceptionUtil.doThrow(ThriftPucketDescriptor.<T>apply(descriptor))
        )
    );
  }
}
