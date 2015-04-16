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

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyDouble;
import static org.mockito.Matchers.doubleThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.management.ManagementFactory;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Test;

/**
 * Test the ORC memory manager.
 */
public class TestMemoryManager {
  private static final double ERROR = 0.000001;

  private static class NullCallback implements MemoryManager.Callback {
    public boolean checkMemory(double newScale) {
      return false;
    }

    @Override
    public void enterLowMemoryMode() throws IOException {
    }
  }

  @Test
  public void testBasics() throws Exception {
    Configuration conf = new Configuration();
    MemoryManager mgr = new MemoryManager(conf);
    NullCallback callback = new NullCallback();
    long poolSize = mgr.getTotalMemoryPool();
    assertEquals(Math.round(ManagementFactory.getMemoryMXBean().
        getHeapMemoryUsage().getMax() * 0.5), poolSize);
    assertEquals(1.0, mgr.getAllocationScale(), 0.00001);
    mgr.addWriter(new Path("p1"), 1000, callback, 1000);
    assertEquals(1.0, mgr.getAllocationScale(), 0.00001);
    mgr.addWriter(new Path("p1"), poolSize / 2, callback, poolSize / 2);
    assertEquals(1.0, mgr.getAllocationScale(), 0.00001);
    mgr.addWriter(new Path("p2"), poolSize / 2, callback, poolSize / 2);
    assertEquals(1.0, mgr.getAllocationScale(), 0.00001);
    mgr.addWriter(new Path("p3"), poolSize / 2, callback, poolSize / 2);
    assertEquals(0.6666667, mgr.getAllocationScale(), 0.00001);
    mgr.addWriter(new Path("p4"), poolSize / 2, callback, poolSize / 2);
    assertEquals(0.5, mgr.getAllocationScale(), 0.000001);
    mgr.addWriter(new Path("p4"), 3 * poolSize / 2, callback, poolSize / 2);
    assertEquals(0.3333333, mgr.getAllocationScale(), 0.000001);
    mgr.removeWriter(new Path("p1"));
    mgr.removeWriter(new Path("p2"));
    assertEquals(0.5, mgr.getAllocationScale(), 0.00001);
    mgr.removeWriter(new Path("p4"));
    assertEquals(1.0, mgr.getAllocationScale(), 0.00001);
  }

  @Test
  public void testConfig() throws Exception {
    Configuration conf = new Configuration();
    conf.set("hive.exec.orc.memory.pool", "0.9");
    MemoryManager mgr = new MemoryManager(conf);
    long mem =
        ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
    System.err.print("Memory = " + mem);
    long pool = mgr.getTotalMemoryPool();
    assertTrue("Pool too small: " + pool, mem * 0.899 < pool);
    assertTrue("Pool too big: " + pool, pool < mem * 0.901);
  }

  private static class DoubleMatcher extends BaseMatcher<Double> {
    final double expected;
    final double error;
    DoubleMatcher(double expected, double error) {
      this.expected = expected;
      this.error = error;
    }

    @Override
    public boolean matches(Object val) {
      double dbl = (Double) val;
      return Math.abs(dbl - expected) <= error;
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("not sufficiently close to ");
      description.appendText(Double.toString(expected));
    }
  }

  private static DoubleMatcher closeTo(double value, double error) {
    return new DoubleMatcher(value, error);
  }

  @Test
  public void testCallback() throws Exception {
    Configuration conf = new Configuration();
    MemoryManager mgr = new MemoryManager(conf);
    long pool = mgr.getTotalMemoryPool();
    MemoryManager.Callback[] calls = new MemoryManager.Callback[20];
    for(int i=0; i < calls.length; ++i) {
      calls[i] = mock(MemoryManager.Callback.class);
      mgr.addWriter(new Path(Integer.toString(i)), pool/4, calls[i], pool / 4);
    }
  }

  private static class MemoryManagerForTest extends MemoryManager {

    public MemoryManagerForTest(Configuration conf) {
      super(conf);
    }

    public double getAllocationMultiplierForPath(Path path) {
      return writerList.get(path).getAllocationMultiplier();
    }

    public void incrementFlushCount(Path path, int increment) {
      writerList.get(path).incrementFlushedCount(increment);
    }
  }

  private void initializeLowMemoryModeTest(MemoryManagerForTest mgr,
      MemoryManager.Callback[] calls) throws Exception {
    long pool = mgr.getTotalMemoryPool();
    for(int i=0; i < calls.length; ++i) {
      calls[i] = mock(MemoryManager.Callback.class);

      // Each writer requests 1/4 of the pool, so by the 5th one, 1/4 of the pool - 1 should be
      // greater than the amount of memory the manager can actually allocate to that pool
      mgr.addWriter(new Path(Integer.toString(i)), pool / 4, calls[i], pool / 4 - 1);
    }
    for (int i = 0; i < 5; i++) {
      verify(calls[i], times(1)).enterLowMemoryMode();
    }
    for (int i = 5; i < calls.length; i++) {
      verify(calls[i], times(0)).enterLowMemoryMode();
    }
    for (int i = 0; i < calls.length / 2; i++) {
      mgr.incrementFlushCount(new Path(Integer.toString(i * 2)), 2);
    }
    // add enough rows to get the memory manager to check the limits twice
    for (int i=0; i < 10000; i++) {
      mgr.addedRow();
    }
  }

  @Test
  public void testLowMemoryMode() throws Exception {
    Configuration conf = new Configuration();
    MemoryManagerForTest mgr = new MemoryManagerForTest(conf);
    MemoryManager.Callback[] calls = new MemoryManager.Callback[8];

    initializeLowMemoryModeTest(mgr, calls);

    for(int i = 0; i < calls.length; i++) {
      if (i % 2 == 0) {
        Assert.assertEquals("Popular writers not getting more memory", 1.5,
            mgr.getAllocationMultiplierForPath(new Path(Integer.toString(i))));
      } else {
        Assert.assertEquals("Unpopular writers hanging onto memory", 0.5,
            mgr.getAllocationMultiplierForPath(new Path(Integer.toString(i))));
      }
    }
  }

  @Test
  public void testLowMemoryModeBidirectional() throws Exception {

    Configuration conf = new Configuration();
    MemoryManagerForTest mgr = new MemoryManagerForTest(conf);
    MemoryManager.Callback[] calls = new MemoryManager.Callback[8];

    initializeLowMemoryModeTest(mgr, calls);

    for (int i = 0; i < calls.length / 2; i++) {
      mgr.incrementFlushCount(new Path(Integer.toString(i * 2)), 2);
    }

    // add enough rows to get the memory manager to check the limits again
    for (int i = 0; i < 10000; i++) {
      mgr.addedRow();
    }

    for(int i = 0; i < calls.length; i++) {
      if (i % 2 == 0) {
        Assert.assertEquals("Popular writers not getting more memory", 1.75,
            mgr.getAllocationMultiplierForPath(new Path(Integer.toString(i))));
      } else {
        Assert.assertEquals("Unpopular writers hanging onto memory", 0.25,
            mgr.getAllocationMultiplierForPath(new Path(Integer.toString(i))));
      }
    }

    // Switch it around
    for (int i = 0; i < calls.length / 2; i++) {
      mgr.incrementFlushCount(new Path(Integer.toString((i * 2) + 1)), 2);
    }

    // add enough rows to get the memory manager to check the limits again
    for (int i = 0; i < 5000; i++) {
      mgr.addedRow();
    }

    for(int i = 0; i < calls.length; i++) {
      if (i % 2 == 0) {
        Assert.assertEquals("Popular writers not getting more memory", 0.875,
            mgr.getAllocationMultiplierForPath(new Path(Integer.toString(i))));
      } else {
        Assert.assertEquals("Unpopular writers hanging onto memory", 1.125,
            mgr.getAllocationMultiplierForPath(new Path(Integer.toString(i))));
      }
    }
  }

  @Test
  public void testLowMemoryModeMinRespected() throws Exception {

    Configuration conf = new Configuration();
    MemoryManagerForTest mgr = new MemoryManagerForTest(conf);
    MemoryManager.Callback[] calls = new MemoryManager.Callback[8];

    initializeLowMemoryModeTest(mgr, calls);

    // Add rows until the allocation is so small, dividing it by 2 again would dip below the min
    int n = 1;
    while ((mgr.getTotalMemoryPool() / 4) * mgr.getAllocationScale() * (0.875 / (2 * n)) >
        OrcConf.getLongVar(conf, OrcConf.ConfVars.HIVE_ORC_FILE_MIN_MEMORY_ALLOCATION)) {
      for (int i = 0; i < 5000; i++) {
        mgr.addedRow();
      }
      n++;
    }

    // Store the old values of the multipliers
    double[] oldMultipliers = new double[calls.length];
    for (int i = 0; i < calls.length; i++) {
      oldMultipliers[i] = mgr.getAllocationMultiplierForPath(new Path(Integer.toString(i)));
    }

    // Add rows to try to tempt the MemoryManager to reallocate the memory again
    for (int i = 0; i < 5000; i++) {
      mgr.addedRow();
    }

    // Verify it didn't
    for(int i = 0; i < calls.length; i++) {
      Assert.assertEquals("Minimum memory allocation not respected", oldMultipliers[i],
          mgr.getAllocationMultiplierForPath(new Path(Integer.toString(i))));
    }
  }
}
