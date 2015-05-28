/*
 * This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 */

package org.voltdb.exportclient.hive;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;

import org.apache.hive.hcatalog.streaming.ConnectionError;
import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.apache.hive.hcatalog.streaming.ImpersonationFailed;
import org.apache.hive.hcatalog.streaming.InvalidPartition;
import org.apache.hive.hcatalog.streaming.InvalidTable;
import org.apache.hive.hcatalog.streaming.PartitionCreationFailed;
import org.apache.hive.hcatalog.streaming.StreamingConnection;
import org.apache.hive.hcatalog.streaming.StreamingException;
import org.apache.hive.hcatalog.streaming.StrictJsonWriter;
import org.apache.hive.hcatalog.streaming.TransactionBatch;

import com.google_voltpatches.common.base.Function;
import com.google_voltpatches.common.collect.FluentIterable;

public class HivePartitionStream implements Closeable {
    private final static HiveExportLogger LOG = new HiveExportLogger();
    final static int BATCH_SIZE = 512;

    final HiveEndPoint m_endPoint;
    final StreamingConnection m_connection;
    TransactionBatch m_batch;

    public HivePartitionStream(HiveEndPoint endPoint) {
        m_endPoint = endPoint;
        try {
            // TODO: may need to pass user impersonation
            m_connection = m_endPoint.newConnection(true);
        } catch (ConnectionError | InvalidPartition | InvalidTable
                | PartitionCreationFailed | ImpersonationFailed
                | InterruptedException e) {
            String msg = "failed to connect to: %s";
            LOG.error(msg, e, endPoint);
            throw new HiveExportException(msg, e, endPoint);
        }
        checkBatch();
    }

    public HiveEndPoint getEndPoint() {
        return m_endPoint;
    }

    private void checkBatch() {
        TransactionBatch oldBatch = null;
        if (m_batch == null || m_batch.remainingTransactions() == 0)  try {
            oldBatch = m_batch;
            m_batch = m_connection.fetchTransactionBatch(BATCH_SIZE, new StrictJsonWriter(m_endPoint));
        } catch (StreamingException | InterruptedException e) {
            String msg = "failed to get transaction batch for %s";
            LOG.error(msg, e, m_endPoint);
            throw new HiveExportException(msg, e, m_endPoint);
        } finally {
            if (oldBatch != null) try {
                oldBatch.close();
            } catch (Exception ignoreIt) {}
        }
    }

    @Override
    public void close() {
        if (m_batch != null)
            try { m_batch.close(); } catch (Exception ignoreIt) {} finally { m_batch = null; }
        try { m_connection.close(); } catch (Exception ignoreIt) {}
    }

    public void write(Collection<String> jsons) {
        if (jsons == null || jsons.isEmpty()) return;

        checkBatch();
        List<byte[]> messages = FluentIterable.from(jsons).transform(asBytes).toList();

        try {
            m_batch.beginNextTransaction();
        } catch (StreamingException | InterruptedException e) {
            String msg = "Failed to initiate transaction against %s";
            LOG.error(msg, e, m_endPoint);
            throw new HiveExportException(msg, e, m_endPoint);
        }
        try {
            m_batch.write(messages);
        } catch (StreamingException | InterruptedException e) {
            String msg = "Failed to write to endpoint \"%s\" messages: %s";
            LOG.error(msg, e, m_endPoint, jsons);
            throw new HiveExportException(msg, e, m_endPoint, jsons);
        }
        try {
            m_batch.commit();
        } catch (StreamingException | InterruptedException e) {
            String msg = "Failed to commit to endpoint \"%s\" messages: %s";
            LOG.error(msg, e, m_endPoint, jsons);
            throw new HiveExportException(msg, e, m_endPoint, jsons);
        }
    }

    final static Function<String,byte[]> asBytes = new Function<String, byte[]>() {
        @Override
        public byte[] apply(String input) {
            return input.getBytes(StandardCharsets.UTF_8);
        }
    };
}
