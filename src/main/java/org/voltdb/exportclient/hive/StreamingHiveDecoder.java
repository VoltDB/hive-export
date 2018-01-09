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

import org.voltdb.VoltType;
import org.voltdb.exportclient.decode.v2.BatchDecoder;

import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.json_voltpatches.JSONException;

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
    public void add(long generation, String tableName, List<VoltType> types, List<String> names, Object[] fields) throws JSONException {
        m_partitionedDecoder.decode(generation, tableName, types, names, m_map, fields);
    }

    @Override
    public Multimap<HiveEndPoint, String> harvest(long generationIgnored) {
        Multimap<HiveEndPoint, String> harvested = m_map.build();
        m_map = ImmutableMultimap.builder();
        return harvested;
    }

    @Override
    public void discard(long genertionIgnored) { }

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
