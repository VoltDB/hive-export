/*
 * This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 */

package org.voltdb.exportclient.hive;

import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.json_voltpatches.JSONException;
import org.voltdb.exportclient.decode.BatchDecoder;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.collect.ImmutableMultimap;
import com.google_voltpatches.common.collect.Multimap;

public class StreamingHiveDecoder implements BatchDecoder<Multimap<HiveEndPoint, String>, JSONException> {

    protected ImmutableMultimap.Builder<HiveEndPoint, String> m_map = ImmutableMultimap.builder();
    final protected PartitionedJsonDecoder m_partitionedDecoder;

    protected StreamingHiveDecoder(PartitionedJsonDecoder partitionedDecoder) {
        m_partitionedDecoder = Preconditions.checkNotNull(partitionedDecoder, "null decoder");
    }

    @Override
    public void add(Object[] fields) throws JSONException {
        m_partitionedDecoder.decode(m_map, fields);
    }

    @Override
    public Multimap<HiveEndPoint, String> harvest() {
        Multimap<HiveEndPoint, String> harvested = m_map.build();
        m_map = ImmutableMultimap.builder();
        return harvested;
    }

    @Override
    public void discard() {
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends PartitionedJsonDecoder.DelegateBuilder {
        private final PartitionedJsonDecoder.Builder m_delegateBuilder;

        protected Builder() {
            super(PartitionedJsonDecoder.builder());
            m_delegateBuilder = getDelegateAs(PartitionedJsonDecoder.Builder.class);
        }

        public StreamingHiveDecoder build() {
            return new StreamingHiveDecoder(m_delegateBuilder.build());
        }
    }
}
