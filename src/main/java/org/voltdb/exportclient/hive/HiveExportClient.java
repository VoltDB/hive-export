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

import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.voltcore.utils.CoreUtils;

import org.voltdb.VoltDB;
import org.voltdb.export.AdvertisedDataSource;
import org.voltdb.exportclient.ExportClientBase;
import org.voltdb.exportclient.ExportDecoderBase;
import org.voltdb.exportclient.ExportRowData;

import org.json_voltpatches.JSONException;

import com.google_voltpatches.common.base.Splitter;
import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableMultimap;
import com.google_voltpatches.common.collect.Multimap;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;

public class HiveExportClient extends ExportClientBase {

    private final static String HIVE_URI_PN = "hive.uri";
    private final static String HIVE_DB_PN = "hive.db";
    private final static String HIVE_TABLE_PN = "hive.table";
    private final static String HIVE_PARTITION_COLUMNS_PN = "hive.partition.columns";
    private final static String TIMEZONE_PN = "timezone";

    private final static Splitter COMMA_SPLITTER = Splitter.on(",").omitEmptyStrings().trimResults();
    private final static Splitter COLUMN_SPLITTER = Splitter.on(":").omitEmptyStrings().trimResults();
    private final static Splitter PIPE_SPLITTER = Splitter.on("|").omitEmptyStrings().trimResults();

    private final static HiveExportLogger LOG = new HiveExportLogger();

    private Multimap<String, String> m_hivePartitionColumns;
    private HiveEndPointFactory m_endPointFactory;
    private TimeZone m_timeZone = VoltDB.REAL_DEFAULT_TIMEZONE;
    private int m_hivePartionCount;

    @Override
    public void configure(Properties config) throws Exception {

        String uri = config.getProperty(HIVE_URI_PN, "");
        String db = config.getProperty(HIVE_DB_PN, "");
        String table = config.getProperty(HIVE_TABLE_PN, "");

        m_endPointFactory = construcHiveEndPointFactory(uri, db, table);

        String timeZoneID = config.getProperty(TIMEZONE_PN, "").trim();
        if (!timeZoneID.isEmpty()) {
            m_timeZone = TimeZone.getTimeZone(timeZoneID);
        }

        String partitionColumns = config.getProperty(HIVE_PARTITION_COLUMNS_PN, "");

        ImmutableMultimap.Builder<String, String> mmbldr = ImmutableMultimap.builder();
        for (String stanza: COMMA_SPLITTER.split(partitionColumns)) {
            List<String> pair = COLUMN_SPLITTER.splitToList(stanza);
            if (pair.size() > 2 || pair.isEmpty()) {
                throw new IllegalArgumentException(
                        "Malformed value \"" + partitionColumns
                      + "\" for property " + HIVE_PARTITION_COLUMNS_PN
                        );
            }
            if (pair.size() == 2) {
                for (String column: PIPE_SPLITTER.split(pair.get(1))) {
                    mmbldr.put(pair.get(0).toUpperCase(), column.toUpperCase());
                }
            }
        }
        m_hivePartitionColumns = mmbldr.build();
        if (!m_hivePartitionColumns.isEmpty()) {
            int partitionCount = m_hivePartitionColumns.get(
                    m_hivePartitionColumns.keySet().iterator().next()
                    ).size()
                    ;
            for (@SuppressWarnings("unused") String tn: m_hivePartitionColumns.keySet()) {
                if (m_hivePartitionColumns.size() != partitionCount) {
                     throw new IllegalArgumentException(
                             "Property " + HIVE_PARTITION_COLUMNS_PN
                             + " contains tables with differing number of columns: "
                             + partitionColumns
                             );
                }
            }
        }
    }

    // this allows easier mocking for unit tests
    HiveEndPointFactory construcHiveEndPointFactory(String uri, String db, String table) {
        return new HiveEndPointFactory(uri, db, table);
    }

    // this allows easier mocking for unit tests
    HiveSink getSink() {
        return HiveSink.instance();
    }

    class HiveExportDecoder extends ExportDecoderBase {
        boolean m_primed = false;
        StreamingHiveDecoder m_decoder;
        final ListeningExecutorService m_es;

        public HiveExportDecoder(AdvertisedDataSource ds) {
            super(ds);
            m_es = CoreUtils.getListeningSingleThreadExecutor(
                    "Hive Export decoder for partition " + ds.partitionId, CoreUtils.MEDIUM_STACK_SIZE);
        }

        final void checkOnFirstRow(ExportRowData row) throws RestartBlockException {
            if (!m_primed) try {
                List<String> partitionColumnNames = ImmutableList.copyOf(
                        m_hivePartitionColumns.get(row.tableName.toUpperCase())
                        );
                if (m_hivePartionCount > 0 && partitionColumnNames.isEmpty()) {
                    throw new IllegalArgumentException(
                            "table " + row.tableName + " is not listed in the \""
                                    + HIVE_PARTITION_COLUMNS_PN + "\" configuration property");
                }
                StreamingHiveDecoder.Builder builder = StreamingHiveDecoder.builder();
                builder
                    .endPointFactory(m_endPointFactory)
                    .partitionColumnNames(partitionColumnNames)
                    .timeZone(m_timeZone)
                    .camelCaseFieldNames(false)
                    .skipInternalFields(true)
                ;
                m_decoder = builder.build();

                String threadName = "Hive Export decoder for partition " + row.partitionId
                        + " table " + row.tableName + " generation " + row.generation;
                Thread.currentThread().setName(threadName);

                m_primed = true;
            } catch (IllegalArgumentException e) {
                LOG.error("Unable to initialize decoder for %s", e, m_endPointFactory);
                throw new RestartBlockException("unable to initialze decoder", e, true);
            }
        }

        @Override
        public ListeningExecutorService getExecutor() {
            return m_es;
        }

        @Override
        public void onBlockStart(ExportRowData row) throws RestartBlockException {
            if (!m_primed) {
                checkOnFirstRow(row);
            }
        }

        @Override
        public boolean processRow(ExportRowData data) throws RestartBlockException {
            if (!m_primed) {
                checkOnFirstRow(data);
            }

            try {
                m_decoder.add(data.generation, data.tableName, data.types, data.names, data.values);
            }
            catch (JSONException e) {
                // non restartable structural failure
                LOG.error("Unable to decode notification", e);
                return false;
            }
            return true;
        }

        @Override
        public void onBlockCompletion(ExportRowData data) throws RestartBlockException {
            try {
                getSink().write(m_decoder.harvest(data.generation));
            } catch (HiveExportException e) {
                throw new RestartBlockException("Hive write fault", e, true);
            }
        }

        @Override
        public void sourceNoLongerAdvertised(AdvertisedDataSource source) {
            getSink().nudge();
            m_es.shutdown();
            try {
                m_es.awaitTermination(365, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                throw new HiveExportException("Interrupted while awaiting executor shutdown", e);
            }
        }
    }

    @Override
    public ExportDecoderBase constructExportDecoder(AdvertisedDataSource source) {
        getSink().nudge();
        return new HiveExportDecoder(source);
    }
}
