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
import org.apache.hive.hcatalog.streaming.HiveEndPoint;
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

    final HiveConf m_conf;
    HiveEndPoint m_endPoint;
    StreamingConnection m_connection;
    StrictJsonWriter m_writer;
    TransactionBatch m_batch;

    public HivePartitionStream(HiveEndPoint endPoint) {
        m_conf = new HiveConf(HivePartitionStream.class);
        m_conf.setVar(HiveConf.ConfVars.METASTOREURIS, endPoint.metaStoreUri);

        connect(endPoint);
        checkBatch();
    }

    protected void connect(HiveEndPoint ep) {
        m_endPoint = new HiveEndPoint(
                ep.metaStoreUri, ep.database, ep.table, ep.partitionVals
                );
        try {
            // TODO: may need to pass user impersonation
            m_connection = m_endPoint.newConnection(true, m_conf);
            m_writer = new StrictJsonWriter(m_endPoint, m_conf);
        } catch (InterruptedException | StreamingException e) {
            String msg = "failed to connect to: %s";
            LOG.error(msg, e, m_endPoint);
            throw new HiveExportException(msg, e, m_endPoint);
        }
    }

    public HiveEndPoint getEndPoint() {
        return m_endPoint;
    }

    private void checkBatch() {
        if (m_batch == null || m_batch.remainingTransactions() == 0)  try {

            if (m_batch != null) try {
                m_batch.close();
            } catch (Exception ignoreIt) {
            } finally {
                m_batch = null;
            }

            int attemptsLeft = 4;
            TransactionBatchUnAvailable retriedException = null;

            ATTEMPT_LOOP: while (--attemptsLeft >= 0) try {

                m_batch = m_connection.fetchTransactionBatch(
                        HIVE_TRANSACTION_BATCH_SIZE, m_writer
                        );

                retriedException = null;
                break ATTEMPT_LOOP;

            } catch (TransactionBatchUnAvailable e) {

                retriedException = e;
                if (attemptsLeft > 0) {
                    Thread.sleep(30);
                } else {
                    close();
                    connect(m_endPoint);
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
            m_writer = null;
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
                connect(m_endPoint);
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
