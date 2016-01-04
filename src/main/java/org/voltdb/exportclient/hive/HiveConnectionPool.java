/*
 * The MIT License (MIT)
 *
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.hive.hcatalog.streaming.HiveEndPoint;

import com.google_voltpatches.common.base.Optional;
import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.cache.CacheBuilder;
import com.google_voltpatches.common.cache.CacheLoader;
import com.google_voltpatches.common.cache.LoadingCache;
import com.google_voltpatches.common.cache.RemovalCause;
import com.google_voltpatches.common.cache.RemovalListener;
import com.google_voltpatches.common.cache.RemovalNotification;

public class HiveConnectionPool {

    private final static HiveExportLogger LOG = new HiveExportLogger();

    final static int POOL_SIZE = Integer.getInteger("HIVE_CONNECTION_POOL_SIZE", 64);
    final static int CONNECTION_TTL = Integer.getInteger("HIVE_CONNECTION_TTL", 30);

    private final LoadingCache<HiveEndPoint, HivePartitionStream> m_pool;

    HiveConnectionPool() {
        m_pool = CacheBuilder
                    .newBuilder()
                    .maximumSize(POOL_SIZE)
                    .expireAfterAccess(CONNECTION_TTL, TimeUnit.MINUTES)
                    .removalListener(connectionRemover)
                    .build(poolLoader);
    }

    private final static CacheLoader<HiveEndPoint, HivePartitionStream> poolLoader =
            new CacheLoader<HiveEndPoint, HivePartitionStream>() {
        @Override
        public HivePartitionStream load(HiveEndPoint endPoint) throws Exception {
            return new HivePartitionStream(endPoint);
        }
    };

    private final static RemovalListener<HiveEndPoint, HivePartitionStream> connectionRemover =
            new RemovalListener<HiveEndPoint, HivePartitionStream>() {
        @Override
        public void onRemoval(
                RemovalNotification<HiveEndPoint, HivePartitionStream> notification) {
            if (notification.getCause() == RemovalCause.SIZE) {
                LOG.warn(
                        "Hive Connection pool reached its size limit, meaning"
                      + " that you are writing concurently to %d"
                      + " hive partitions. You may need to restart VoltDB"
                      + " to increase the pool size by setting the"
                      + " HIVE_CONNECTION_POOL_SIZE property to a higher value, and"
                      + " making sure you are allocating enough file descriptors "
                      + " to the VoltDB process", POOL_SIZE);
            }
            notification.getValue().close();
        }
    };

    public Optional<HivePartitionStream> getOptionally(HiveEndPoint key) {
        return Optional.fromNullable(
                m_pool.getIfPresent(
                        Preconditions.checkNotNull(key, "provided null lookup key")
               ));
    }

    public HivePartitionStream get(HiveEndPoint key) {
        try {
            return m_pool.get(Preconditions.checkNotNull(key, "provided null lookup key"));
        } catch (ExecutionException e) {
            if ((e.getCause() instanceof HiveExportException)) {
                throw (HiveExportException)e.getCause();
            } else {
                String msg = "Unable to get partition stream for %s";
                LOG.error(msg, e.getCause(), key);
                throw new HiveExportException(msg, e.getCause(), key);
            }
        }
    }

    public void evict(HiveEndPoint key) {
        m_pool.invalidate(Preconditions.checkNotNull(key, "provided null lookup key"));
    }

    public void put(HiveEndPoint key, HivePartitionStream value) {
        m_pool.put(
                Preconditions.checkNotNull(key, "null key"),
                Preconditions.checkNotNull(value, "null value")
                );
    }

    public void nudge() {
        m_pool.cleanUp();;
    }
}
