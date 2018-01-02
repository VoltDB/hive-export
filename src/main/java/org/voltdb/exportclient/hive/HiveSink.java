/*
 * The MIT License (MIT)
 *
 * Copyright (C) 2008-2018 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package org.voltdb.exportclient.hive;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.voltcore.utils.CoreUtils;

import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.Multimap;
import com.google_voltpatches.common.util.concurrent.Futures;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;

public class HiveSink {
    private final static HiveExportLogger LOG = new HiveExportLogger();

    public final static int HIVE_CONCURRENT_WRITERS = Integer.getInteger("HIVE_CONCURRENT_WRITERS", 4);

    private final List<ListeningExecutorService> m_executors;
    private final HiveConnectionPool m_pool = new HiveConnectionPool();

    private HiveSink() {
        ImmutableList.Builder<ListeningExecutorService> lbldr = ImmutableList.builder();
        for (int i = 0; i < HIVE_CONCURRENT_WRITERS; ++i) {
            String threadName = "Hive Export Sink Writer " + i;
            lbldr.add(CoreUtils.getListeningSingleThreadExecutor(threadName,CoreUtils.MEDIUM_STACK_SIZE));
        }
        m_executors = lbldr.build();
    }

    private final static class Holder {
        private final static HiveSink instance = new HiveSink();
    }

    public final static HiveSink instance() {
        return Holder.instance;
    }

    ListenableFuture<?> asWriteTask(final HiveEndPoint endPoint, final Collection<String> records) {
        final int hashed = Math.abs(endPoint.partitionVals.hashCode() % HIVE_CONCURRENT_WRITERS);
        if (m_executors.get(hashed).isShutdown()) {
            return Futures.immediateFailedFuture(new HiveExportException("hive sink executor is shut down"));
        }
        return m_executors.get(hashed).submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                HivePartitionStream stream = m_pool.get(endPoint);
                try {
                    stream.write(records);
                } catch (HiveExportException e) {
                    m_pool.evict(endPoint);
                    throw e;
                }
                return null;
            }
        });
    }

    public void write(Multimap<HiveEndPoint, String> records) {
        List<ListenableFuture<?>> tasks = new ArrayList<>();
        for (HiveEndPoint ep: records.keySet()) {
            tasks.add(asWriteTask(ep, records.get(ep)));
        }
        try {
            Futures.allAsList(tasks).get();
        } catch (InterruptedException e) {
            String msg = "Interrupted write for message %s";
            LOG.error(msg, e, records);
            throw new HiveExportException(msg, e, records);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof HiveExportException) {
                throw (HiveExportException)e.getCause();
            }
            String msg = "Fault on write for message %s";
            LOG.error(msg, e, records);
            throw new HiveExportException(msg, e, records);
        }
    }

    public void nudge() {
        m_pool.nudge();
    }
}
