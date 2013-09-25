package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.serde2.SerDeStats;

public interface SerDeStatsSupplier
{
  SerDeStats getStats();
}
