/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.jvm;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.OSMetricsGeneratorFactory;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.ScheduledMetricCollectorsExecutor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatExceptionCode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.core.Util;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.sun.tools.attach.VirtualMachine;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import sun.tools.attach.HotSpotVirtualMachine;

/** Traverses and prints the stack traces for all Java threads in the remote VM */
public class ThreadList {
  private static final Map<Long, String> jTidNameMap = new ConcurrentHashMap<>();
  private static final Map<Long, ThreadState> nativeTidMap = new ConcurrentHashMap<>();
  private static final Map<Long, ThreadState> oldNativeTidMap = new ConcurrentHashMap<>();
  private static final Map<Long, ThreadState> jTidMap = new ConcurrentHashMap<>();
  private static final Map<String, ThreadState> nameMap = new ConcurrentHashMap<>();
  private static final String pid = OSMetricsGeneratorFactory.getInstance().getPid();
  static final Logger LOGGER = LogManager.getLogger(ThreadList.class);
  static final int samplingInterval =
      MetricsConfiguration.CONFIG_MAP.get(ThreadList.class).samplingInterval;

  // This value controls how often we do the thread dump.
  private static final long minRunInterval = samplingInterval;
  private static final ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
  private static final Pattern linePattern = Pattern.compile("\"([^\"]*)\"");
  private static long lastRunTime = 0;

  private static Lock vmAttachLock = new ReentrantLock();

  public static class ThreadState {
    public long javaTid;
    public long nativeTid;
    public long heapUsage;
    public String threadName;
    public String tState;
    public Thread.State state;
    public long blockedCount;
    public long blockedTime;

    public double heapAllocRate;
    public double avgBlockedTime;

    ThreadState() {
      javaTid = -1;
      nativeTid = -1;
      heapUsage = -1;
      heapAllocRate = 0;
      blockedCount = 0;
      blockedTime = 0;
      avgBlockedTime = 0;
      threadName = "";
      tState = "";
    }

    @Override
    public String toString() {
      return new StringBuilder()
          .append("javatid:")
          .append(javaTid)
          .append(" nativetid:")
          .append(nativeTid)
          .append(" name:")
          .append(threadName)
          .append(" state:")
          .append(tState)
          .append("(")
          .append(state)
          .append(")")
          .append(" heaprate: ")
          .append(heapAllocRate)
          .append(" bTime: ")
          .append(avgBlockedTime)
          .append(":")
          .append(blockedCount)
          .toString();
    }
  }

  /**
   * This is called from OSMetricsCollector#collectMetrics. So this is not called
   * in the critical path of ES request handling. Even for the collector thread,
   * we do a timed wait to acquire this lock and move on if we could not get it.
   * @return A hashmap of threadId to threadState.
   */
  public static Map<Long, ThreadState> getNativeTidMap() {
    if (vmAttachLock.tryLock()) {
      try {
        // Thread dumps are expensive and therefore we make sure that at least
        // minRunInterval milliseconds have elapsed between two attempts.
        if (System.currentTimeMillis() > lastRunTime + minRunInterval) {
          runThreadDump(pid, new String[0]);
        }
      } finally {
        vmAttachLock.unlock();
      }
    } else {
      StatsCollector.instance().logException(StatExceptionCode.JVM_ATTACH_LOCK_ACQUISITION_FAILED);
    }

    // - sending a copy so that if runThreadDump next iteration clears it; caller still has the
    // state at the call time
    // - not too expensive as this is only being called from Scheduled Collectors (only once in
    // few seconds)
    return new HashMap<>(nativeTidMap);
  }

  /**
   * This method is called from the critical bulk and search paths which PA
   * intercepts. This method used to try to do a thread dump if it could not
   * find the information about the thread in question. The thread dump is an
   * expensive operation and can stall see VirtualMachineImpl#VirtualMachineImpl()
   * for jdk-11 u06. We don't want the ES threads to pay the price. We skip this
   * iteration and then hopefully in the next call to getNativeTidMap(), the
   * OSMetricsCollector#collectMetrics will fill the jTidMap. This transfers the
   * responsibility from the ES threads to the PA collector threads.
   *
   * @param threadId The threadId of the current thread.
   * @return If we have successfully captured the ThreadState, then we emit it or Null otherwise.
   */
  public static ThreadState getThreadState(long threadId) {
    ThreadState retVal = jTidMap.get(threadId);
    if (retVal == null) {
      StatsCollector.instance().logException(StatExceptionCode.NO_THREAD_STATE_INFO);
    }
    return retVal;
  }

  // Attach to pid and perform a thread dump
  private static void runAttachDump(String pid, String[] args) {
    VirtualMachine vm = null;
    try {
      vm = VirtualMachine.attach(pid);
    } catch (Exception ex) {
      if (ex.getMessage().contains("java_pid")) {
        StatsCollector.instance().logException(StatExceptionCode.JVM_ATTACH_ERROR_JAVA_PID_FILE_MISSING);
      } else {
        StatsCollector.instance().logException(StatExceptionCode.JVM_ATTACH_ERROR);
      }
      // If the thread dump failed then we clean up the old map. So, next time when the collection
      // happens as it would after a bootup.
      oldNativeTidMap.clear();
      return;
    }

    try (InputStream in = ((HotSpotVirtualMachine) vm).remoteDataDump(args); ) {
      createMap(in);
    } catch (Exception ex) {
      StatsCollector.instance().logException(StatExceptionCode.JVM_ATTACH_ERROR);
      oldNativeTidMap.clear();
    }

    try {
      vm.detach();
      StatsCollector.instance().logException(StatExceptionCode.JVM_THREAD_DUMP_SUCCESSFUL);
    } catch (Exception ex) {
      StatsCollector.instance().logException(StatExceptionCode.JVM_ATTACH_ERROR);
    }
  }

  public static void parseAllThreadInfos(ThreadInfo[] infos) {
    for (ThreadInfo info : infos) {
      try {
        parseThreadInfo(info);
      } catch (Exception ex) {
        // If the ids provided to the getThreadInfo() call are not valid ids or the threads no
        //longer exists, then the corresponding info object will contain null.
        StatsCollector.instance()
                .logException(StatExceptionCode.JVM_THREAD_ID_NO_LONGER_EXISTS);
      }
    }
  }

  public static ThreadInfo[] getAllThreadInfos() {
    long[] ids = threadBean.getAllThreadIds();
    return threadBean.getThreadInfo(ids);
  }

  // ThreadMXBean-based info for tid, name and allocs
  private static void runMXDump() {
    ThreadInfo[] infos = getAllThreadInfos();
    parseAllThreadInfos(infos);
    ThreadHistory.cleanup();
  }

  private static void parseThreadInfo(final ThreadInfo info) {
    long id = info.getThreadId();
    String name = info.getThreadName();
    Thread.State state = info.getThreadState();

    // following captures cumulative allocated bytes + TLAB used bytes
    // and it is cumulative
    long mem = ((com.sun.management.ThreadMXBean) threadBean).getThreadAllocatedBytes(id);

    ThreadState t = jTidMap.get(id);
    if (t == null) {
      return;
    }
    t.heapUsage = mem;
    t.state = state;
    t.blockedCount = info.getBlockedCount();
    t.blockedTime = info.getBlockedTime();
    ThreadHistory.add(t.nativeTid, (state == Thread.State.BLOCKED) ? samplingInterval : 0);

    long curRunTime = System.currentTimeMillis();
    ThreadState oldt = oldNativeTidMap.get(t.nativeTid);
    if (curRunTime > lastRunTime && oldt != null) {
      t.heapAllocRate =
              Math.max(t.heapUsage - oldt.heapUsage, 0) * 1.0e3 / (curRunTime - lastRunTime);
      if (t.blockedTime != -1 && t.blockedCount > oldt.blockedCount) {
        t.avgBlockedTime =
                1.0e-3 * (t.blockedTime - oldt.blockedTime) / (t.blockedCount - oldt.blockedCount);
      } else {
        CircularLongArray arr = ThreadHistory.tidHistoryMap.get(t.nativeTid);
        // NOTE: this is an upper bound
        if (arr != null) {
          t.avgBlockedTime = 1.0 * arr.getAvgValue() / samplingInterval;
        }
      }
    }
    jTidNameMap.put(id, name);
  }

  static void runThreadDump(String pid, String[] args) {
    String currentThreadName = Thread.currentThread().getName();
    assert currentThreadName.startsWith(ScheduledMetricCollectorsExecutor.COLLECTOR_THREAD_POOL_NAME)
                   || currentThreadName.equals(ScheduledMetricCollectorsExecutor.class.getSimpleName()) :
            String.format("Thread dump called from a non os collector thread: %s", currentThreadName);
    jTidNameMap.clear();
    oldNativeTidMap.putAll(nativeTidMap);
    nativeTidMap.clear();
    jTidMap.clear();
    nameMap.clear();

    // TODO: make this map update atomic
    Util.invokePrivileged(() -> runAttachDump(pid, args));
    // oldNativeTidMap gets cleared if the attach Fails, so that the
    // metrics collection starts as it would after a restart.
    if (!oldNativeTidMap.isEmpty()) {
      runMXDump();
    }
    lastRunTime = System.currentTimeMillis();
  }

  private static void parseLine(String line) {
    String[] tokens = line.split(" os_prio=[0-9]* ");
    ThreadState t = new ThreadState();
    t.javaTid = -1;

    Matcher m = linePattern.matcher(tokens[0]);
    if (!m.find()) {
      t.threadName = tokens[0];
    } else {
      t.threadName = m.group(1);
      if (!tokens[0].equals("\"" + t.threadName + "\"")) {
        t.javaTid =
            Long.parseLong(
                tokens[0]
                    .split(Pattern.quote("\"" + t.threadName + "\" "))[1]
                    .split(" ")[0]
                    .split("#")[1]);
      }
    }

    tokens = tokens[1].split(" ");
    for (String token : tokens) {
      String[] keyValuePare = token.split("=");
      if (keyValuePare.length < 2) {
        continue;
      }
      if (t.javaTid == -1 && keyValuePare[0].equals("tid")) {
        t.javaTid = Long.decode(keyValuePare[1]);
      }
      if (keyValuePare[0].equals("nid")) {
        t.nativeTid = Long.decode(keyValuePare[1]);
      }
    }
    t.tState = tokens[2]; // TODO: stuff like "in Object.wait()"
    nativeTidMap.put(t.nativeTid, t);
    jTidMap.put(t.javaTid, t);
    nameMap.put(t.threadName, t); // XXX: we assume no collisions
  }

  private static void createMap(InputStream in) throws Exception {
    BufferedReader br = new BufferedReader(new InputStreamReader(in));
    String line = null;
    while ((line = br.readLine()) != null) {
      if (line.contains("tid=")) {
        parseLine(line);
      }
    }
  }

  // currently stores thread states to track locking periods
  static class ThreadHistory {
    public static Map<Long, CircularLongArray> tidHistoryMap = new HashMap<>();
    private static final int HISTORY_SIZE = 60; // 60 * samplingInterval

    public static void add(long tid, long value) {
      CircularLongArray arr = tidHistoryMap.get(tid);
      if (arr == null) {
        arr = new CircularLongArray(HISTORY_SIZE);
        arr.add(value);
        tidHistoryMap.put(tid, arr);
      } else {
        arr.add(value);
      }
    }

    public static void cleanup() {
      long curTime = System.currentTimeMillis();
      for (Iterator<Map.Entry<Long, CircularLongArray>> it = tidHistoryMap.entrySet().iterator();
          it.hasNext(); ) {
        Map.Entry<Long, CircularLongArray> me = it.next();
        CircularLongArray arr = me.getValue();
        // delete items updated older than 300s
        if (curTime - arr.lastWriteTimestamp > HISTORY_SIZE * samplingInterval * 1.0e3) {
          it.remove();
        }
      }
    }
  }

  // models a fixed-capacity queue that is append-only
  // not thread-safe
  static class CircularLongArray {
    ArrayList<Long> list = null;
    public long lastWriteTimestamp;
    private long totalValue;
    private int startidx;
    private int capacity;

    CircularLongArray(int capacity) {
      list = new ArrayList<>(capacity);
      this.capacity = capacity;
      totalValue = 0;
      startidx = 0;
      lastWriteTimestamp = 0;
    }

    public boolean add(long e) {
      lastWriteTimestamp = System.currentTimeMillis();
      if (list.size() < capacity) {
        // can only happen if startidx == 0
        if (startidx != 0) {
          return false;
        } else {
          totalValue += e;
          return list.add(e);
        }
      }
      totalValue -= list.get(startidx);
      totalValue += e;
      list.set(startidx, e);
      startidx = (startidx + 1) % capacity;
      return true;
    }

    public double getAvgValue() {
      return list.size() == 0 ? 0 : 1.0 * totalValue / list.size();
    }
  }
}
