//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.hive.orc;

import com.facebook.hive.orc.statistics.ColumnStatisticsImpl;
import com.facebook.hive.orc.statistics.DoubleColumnStatistics;
import com.facebook.hive.orc.statistics.IntegerColumnStatistics;
import com.facebook.hive.orc.statistics.StringColumnStatistics;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

/**
 * Test ColumnStatisticsImpl for ORC.
 */
public class TestColumnStatistics {

  @Test
  public void testIntegerStatisticsMerge() throws Exception {
    ObjectInspector inspector =
        PrimitiveObjectInspectorFactory.javaIntObjectInspector;

    ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(inspector);
    ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(inspector);
    stats1.updateInteger(10);
    stats1.updateInteger(10);
    stats2.updateInteger(1);
    stats2.updateInteger(1000);
    stats1.merge(stats2);
    IntegerColumnStatistics typed = (IntegerColumnStatistics) stats1;
    assertEquals(1, typed.getMinimum());
    assertEquals(1000, typed.getMaximum());
  }

  @Test
  public void testDoubleStatisticsMerge() throws Exception {
    ObjectInspector inspector =
        PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;

    ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(inspector);
    ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(inspector);
    stats1.updateDouble(10.0);
    stats1.updateDouble(100.0);
    stats2.updateDouble(1.0);
    stats2.updateDouble(1000.0);
    stats1.merge(stats2);
    DoubleColumnStatistics typed = (DoubleColumnStatistics) stats1;
    assertEquals(1.0, typed.getMinimum(), 0.001);
    assertEquals(1000.0, typed.getMaximum(), 0.001);
  }

  @Test
  public void testStringStatisticsMerge() throws Exception {
    ObjectInspector inspector =
        PrimitiveObjectInspectorFactory.javaStringObjectInspector;

    ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(inspector);
    ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(inspector);
    stats1.updateString("bob");
    stats1.updateString("david");
    stats1.updateString("charles");
    stats2.updateString("anne");
    stats2.updateString("erin");
    stats1.merge(stats2);
    StringColumnStatistics strStats = (StringColumnStatistics) stats1;
    assertEquals("anne", strStats.getMinimum());
    assertEquals("erin", strStats.getMaximum());
  }
}
