package org.apache.parquet.hadoop

import org.apache.parquet.hadoop.api.ReadSupport
import org.apache.parquet.filter2.compat.FilterCompat._

class InternalParquetRecordReaderWrapper[T](readSupport: ReadSupport[T], filter: Filter)
  extends InternalParquetRecordReader[T](readSupport, filter) {}
