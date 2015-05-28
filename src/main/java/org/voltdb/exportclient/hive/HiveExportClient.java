/*
 * This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 */

package org.voltdb.exportclient.hive;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

import org.json_voltpatches.JSONException;
import org.voltdb.VoltDB;
import org.voltdb.export.AdvertisedDataSource;
import org.voltdb.exportclient.ExportClientBase;
import org.voltdb.exportclient.ExportDecoderBase;

import com.google_voltpatches.common.base.Splitter;
import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableMultimap;
import com.google_voltpatches.common.collect.Multimap;

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

        public HiveExportDecoder(AdvertisedDataSource ds) {
            super(ds);
        }

        final void checkOnFirstRow() throws RestartBlockException {
            if (!m_primed) try {
                List<String> partitionColumnNames = ImmutableList.copyOf(
                        m_hivePartitionColumns.get(m_source.tableName.toUpperCase())
                        );
                if (m_hivePartionCount > 0 && partitionColumnNames.isEmpty()) {
                    throw new IllegalArgumentException(
                            "table " + m_source.tableName + " is not listed in the \""
                                    + HIVE_PARTITION_COLUMNS_PN + "\" configuration property");
                }
                StreamingHiveDecoder.Builder builder = StreamingHiveDecoder.builder();
                builder
                    .endPointFactory(m_endPointFactory)
                    .partitionColumnNames(partitionColumnNames)
                    .timeZone(m_timeZone)
                    .camelCaseFieldNames(false)
                    .columnNames(m_source.columnNames)
                    .columnTypes(m_source.columnTypes)
                    .skipInternalFields(true)
                ;
                m_decoder = builder.build();
                m_primed = true;
            } catch (IllegalArgumentException e) {
                LOG.error("Unable to initialize decoder for %s", e, m_endPointFactory);
                throw new RestartBlockException("unable to initialze decoder", e, true);
            }
        }

        @Override
        public void onBlockStart() throws RestartBlockException {
            if (!m_primed) checkOnFirstRow();
        }

        @Override
        public boolean processRow(int rowSize, byte[] rowData) throws RestartBlockException {
            if (!m_primed) checkOnFirstRow();

            try {
                m_decoder.add(decodeRow(rowData).values);
            } catch (IOException|JSONException e) {
                // non restartable structural failure
                LOG.error("Unable to decode notification", e);
                return false;
            }
            return true;
        }

        @Override
        public void onBlockCompletion() throws RestartBlockException {
            try {
                getSink().write(m_decoder.harvest());
            } catch (HiveExportException e) {
                throw new RestartBlockException("Hive write fault", e, true);
            }
        }

        @Override
        public void sourceNoLongerAdvertised(AdvertisedDataSource source) {
        }
    }

    @Override
    public ExportDecoderBase constructExportDecoder(AdvertisedDataSource source) {
        return new HiveExportDecoder(source);
    }
}
