package com.intenthq.pucket

import java.util.logging.{Level, Logger, LogManager}

import org.apache.parquet.Log
import org.slf4j.bridge.SLF4JBridgeHandler

trait TestLogging {
  SLF4JBridgeHandler.removeHandlersForRootLogger()
  LogManager.getLogManager.reset()
  SLF4JBridgeHandler.install()
  Class.forName(classOf[Log].getName)
  val parquetLogger = Logger.getLogger("parquet")
  parquetLogger.getHandlers.foreach(parquetLogger.removeHandler)
  if (!parquetLogger.getUseParentHandlers) parquetLogger.setUseParentHandlers(true)
}
