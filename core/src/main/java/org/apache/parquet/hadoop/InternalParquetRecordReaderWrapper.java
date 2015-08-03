package org.apache.parquet.hadoop;

import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.api.ReadSupport;

public class InternalParquetRecordReaderWrapper<T> extends InternalParquetRecordReader<T> {
  public InternalParquetRecordReaderWrapper(final ReadSupport<T> readSupport,
                                            final FilterCompat.Filter filter) {
    super(readSupport, filter);
  }

  public InternalParquetRecordReaderWrapper(final ReadSupport<T> readSupport) {
    super(readSupport);
  }
}
