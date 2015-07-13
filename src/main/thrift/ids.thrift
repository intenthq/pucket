namespace * com.intenthq.ids.model.thrift


struct ThriftValue {
  1: string stringValue;
  2: i64 integerValue;
  3: double fractionValue;
  4: bool booleanValue;
  6: i64 dateValue; //millisecond EPOCH
}