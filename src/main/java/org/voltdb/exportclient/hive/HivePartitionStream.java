/*
 * This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 */

package org.voltdb.exportclient.hive;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
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
import org.apache.hive.hcatalog.streaming.TransactionBatchUnAvailable;

import com.google_voltpatches.common.base.Function;
import com.google_voltpatches.common.collect.FluentIterable;

public class HivePartitionStream implements Closeable {
    private final static HiveExportLogger LOG = new HiveExportLogger();
    final static int HIVE_TRANSACTION_BATCH_SIZE =
            Integer.getInteger("HIVE_TRANSACTION_BATCH_SIZE", 64);

    final HiveEndPoint m_endPoint;
    final HiveConf m_conf;
    StreamingConnection m_connection;
    TransactionBatch m_batch;

    public HivePartitionStream(HiveEndPoint endPoint) {
        m_conf = new HiveConf(HivePartitionStream.class);
        m_conf.setVar(HiveConf.ConfVars.METASTOREURIS, endPoint.metaStoreUri);
        m_endPoint = endPoint;

        connect();
        checkBatch();
    }

    protected void connect() {
        try {
            // TODO: may need to pass user impersonation
            m_connection = m_endPoint.newConnection(true, m_conf);
        } catch (ConnectionError | InvalidPartition | InvalidTable
                | PartitionCreationFailed | ImpersonationFailed
                | InterruptedException e) {
            String msg = "failed to connect to: %s";
            LOG.error(msg, e, m_endPoint);
            throw new HiveExportException(msg, e, m_endPoint);
        }
    }

    public HiveEndPoint getEndPoint() {
        return m_endPoint;
    }

    private void checkBatch() {
        TransactionBatch oldBatch = null;
        if (m_batch == null || m_batch.remainingTransactions() == 0)  try {

            oldBatch = m_batch;
            int attemptsLeft = 4;
            TransactionBatchUnAvailable retriedException = null;

            ATTEMPT_LOOP: while (--attemptsLeft >= 0) try {
                m_batch = m_connection.fetchTransactionBatch(
                        HIVE_TRANSACTION_BATCH_SIZE,
                        new StrictJsonWriter(m_endPoint)
                        );
                retriedException = null;
                break ATTEMPT_LOOP;
            } catch (TransactionBatchUnAvailable e) {
                retriedException = e;
                if (attemptsLeft > 0) {
                    Thread.sleep(30);
                } else {
                    close(); oldBatch = null;
                    connect();
                }
            }
            if (retriedException != null) {
                throw retriedException;
            }
        } catch (StreamingException | InterruptedException e) {
            close();
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
        if (m_batch != null) try {
            m_batch.close();
        } catch (Exception ignoreIt) {

        } finally {
            m_batch = null;
        }
        if (m_connection != null) try {
            m_connection.close();
        } catch (Exception ignoreIt) {
        } finally {
            m_connection = null;
        }
    }

    public void write(Collection<String> jsons) {
        if (jsons == null || jsons.isEmpty()) return;

        checkBatch();
        List<byte[]> messages = FluentIterable.from(jsons).transform(asBytes).toList();

        int attemptsLeft = 3;
        StreamingException retriedException = null;
        try {
            ATTEMPT_LOOP: while (--attemptsLeft >= 0) try {

                m_batch.beginNextTransaction();
                m_batch.write(messages);
                m_batch.commit();

                retriedException = null;
                break ATTEMPT_LOOP;

            } catch (StreamingException e) {
                retriedException = e;

                close();
                connect();
                checkBatch();
            }
            if (retriedException != null) {
                throw retriedException;
            }
        } catch (StreamingException | InterruptedException e) {
            String msg = "Failed to write to endpoint \"%s\"";
            LOG.error(msg, e, m_endPoint);
            throw new HiveExportException(msg, e, m_endPoint);
        }
    }

    final static Function<String,byte[]> asBytes = new Function<String, byte[]>() {
        @Override
        public byte[] apply(String input) {
            return input.getBytes(StandardCharsets.UTF_8);
        }
    };
}
