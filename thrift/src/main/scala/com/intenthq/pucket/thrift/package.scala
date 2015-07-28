package com.intenthq.pucket

import org.apache.thrift.{TBase, TFieldIdEnum}

package object thrift {
  type Thrift = TBase[_ <: TBase[_, _], _ <: TFieldIdEnum]
}
