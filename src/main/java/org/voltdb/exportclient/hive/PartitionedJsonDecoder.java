/*
 * The MIT License (MIT)
 *
 * Copyright (C) 2008-2017 VoltDB Inc.
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

import static org.voltdb.exportclient.decode.v2.RowDecoder.Builder.camelCaseNameLowerFirst;

import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;

import org.voltdb.VoltType;
import org.voltdb.common.Constants;
import org.voltdb.exportclient.decode.v2.DecodeType;
import org.voltdb.exportclient.decode.v2.DecodeType.SimpleVisitor;
import org.voltdb.exportclient.decode.v2.FieldDecoder;
import org.voltdb.exportclient.decode.v2.RowDecoder;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.Encoder;

import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONWriter;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.collect.FluentIterable;
import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableMultimap;

public class PartitionedJsonDecoder
    extends RowDecoder<ImmutableMultimap.Builder<HiveEndPoint, String>, JSONException> {

    final protected SimpleDateFormat m_dateFormatter =
            new SimpleDateFormat(Constants.ODBC_DATE_FORMAT_STRING);
//    protected String [] m_columnNames;
    protected Map<Long, JsonFieldDecoder []> m_fieldDecoders = new HashMap<>();
    protected boolean m_primed = false;
    protected final List<String> m_partitionColumns;
    protected final StringWriter m_writer;
    protected final String m_unspecifiedToken;
    protected final HiveEndPointFactory m_endPointFactory;
    protected final boolean m_camelcaseNames;

    protected PartitionedJsonDecoder(
            boolean camelcaseNames,
            List<String> partitionColumnNames,
            int firstFieldOffset,
            TimeZone timeZone,
            String unspecifiedToken,
            HiveEndPointFactory endPointFactory) {

        super(firstFieldOffset);

        HiveEndPointFactory.checkPartitionValues(partitionColumnNames);

        Preconditions.checkArgument(
                unspecifiedToken != null && !unspecifiedToken.trim().isEmpty(),
                "unspecified token is null or empty"
                );
        m_unspecifiedToken = unspecifiedToken;
        m_dateFormatter.setTimeZone(Preconditions.checkNotNull(timeZone, "timezone is null"));
        m_endPointFactory = Preconditions.checkNotNull(endPointFactory, "endPointFactory is null");
        m_partitionColumns = partitionColumnNames;
        m_writer = new StringWriter(4096);
        m_camelcaseNames = camelcaseNames;
    }

    @Override
    public ImmutableMultimap.Builder<HiveEndPoint, String> decode(
            long generation,
            String tableName,
            List<VoltType> types,
            List<String> names,
            ImmutableMultimap.Builder<HiveEndPoint, String> to,
            Object[] fields) throws JSONException {

        JsonFieldDecoder[] fieldDecoders = m_fieldDecoders.get(generation);

        if (fieldDecoders == null) {
            List<String> columnNames = names;
            if (m_camelcaseNames) {
                columnNames = FluentIterable.from(names)
                      .transform(camelCaseNameLowerFirst)
                      .toList();
            }

            Map <String, DecodeType> typeMap = getTypeMap(generation, types, names);
            Preconditions.checkArgument(
                  columnNames.containsAll(m_partitionColumns),
                  "partition columns %s are not in table columns %s",
                  m_partitionColumns, columnNames
                  );
            for (String partitionColumn: m_partitionColumns) {
                if (typeMap.get(partitionColumn) != DecodeType.STRING) {
                    throw new IllegalArgumentException(
                            "partition column \"" + partitionColumn + "\" must be of VARCHAR type, "
                            + "but it is of type " + typeMap.get(partitionColumn)
                            );
                }
            }

            int fieldCount = 0;
//            m_columnNames = new String[typeMap.size()];
            fieldDecoders = new JsonFieldDecoder[typeMap.size()];;
            for (Entry<String, DecodeType> e: typeMap.entrySet()) {
                final String columnName = e.getKey().intern();
//                m_columnNames[fieldCount] = columnName;
                fieldDecoders[fieldCount++] = e.getValue()
                        .accept(decodingVisitor, columnName, null)
                        ;
            }
        }

        if (to == null) {
            to = ImmutableMultimap.builder();
        }
        Decoded decodeTo = new Decoded();
        decodeTo.stringer.object();
        for (
                int i = m_firstFieldOffset, j = 0;
                i < fields.length && j < fieldDecoders.length;
                ++i, ++j
        ) {
            fieldDecoders[j].decode(decodeTo,fields[i]);
        }
        decodeTo.stringer.endObject();
        decodeTo.asEntryTo(to);

        return to;
    }

    HiveEndPointFactory getEndPointFactory() {
        return m_endPointFactory;
    }

    private class Decoded {
        private final Map<String, String> partitions = new LinkedHashMap<>();
        private final JSONWriter stringer = new JSONWriter(m_writer);

        private Decoded() {
            for (String column: m_partitionColumns) {
                partitions.put(column, m_unspecifiedToken);
            }
        }

        private void asEntryTo(ImmutableMultimap.Builder<HiveEndPoint, String> builder) {
            String json = m_writer.toString();
            List<String> partitionValues = ImmutableList.copyOf(partitions.values());
            builder.put(getEndPointFactory().endPointFor(partitionValues), json);

            partitions.clear();
            m_writer.getBuffer().setLength(0);
        }
    }

    static abstract class JsonFieldDecoder implements FieldDecoder<Decoded, JSONException> {
        protected final String m_fieldName;

        JsonFieldDecoder(String fieldName) {
            m_fieldName = fieldName;
        }
    }

    final SimpleVisitor<JsonFieldDecoder, String> decodingVisitor =
            new SimpleVisitor<JsonFieldDecoder, String>() {

        JsonFieldDecoder defaultDecoder(String p) {
            return new JsonFieldDecoder(p) {
                @Override
                public final void decode(Decoded to, Object v) throws JSONException {
                    to.stringer.key(m_fieldName).value(v);
                }
            };
        }

        @Override
        public JsonFieldDecoder visitTinyInt(String p, Object v) {
            return defaultDecoder(p);
        }

        @Override
        public JsonFieldDecoder visitSmallInt(String p, Object v) {
            return defaultDecoder(p);
        }

        @Override
        public JsonFieldDecoder visitInteger(String p, Object v) {
            return defaultDecoder(p);
        }

        @Override
        public JsonFieldDecoder visitBigInt(String p, Object v) {
            return defaultDecoder(p);
        }

        @Override
        public JsonFieldDecoder visitFloat(String p, Object v) {
            return defaultDecoder(p);
        }

        @Override
        public JsonFieldDecoder visitTimestamp(String p, Object v) {
            return new JsonFieldDecoder(p) {
                @Override
                public final void decode(Decoded to, Object v)
                        throws JSONException {
                    String formatted = null;
                    if (v != null) {
                        TimestampType ts = (TimestampType)v;
                        formatted = m_dateFormatter.format(ts.asApproximateJavaDate());
                    }
                    to.stringer.key(m_fieldName).value(formatted);
                }
            };
        }

        @Override
        public JsonFieldDecoder visitString(String p, Object v) {
            if (m_partitionColumns.contains(p)) {
                return new JsonFieldDecoder(p) {
                    @Override
                    public final void decode(Decoded to, Object v) throws JSONException {
                        String value = (String)v;
                        if (!HiveEndPointFactory.validPartitionValue.apply(value)) {
                            value = m_unspecifiedToken;
                        }
                        to.partitions.put(m_fieldName, value);
                    }
                };
            } else {
                return defaultDecoder(p);
            }
        }

        @Override
        public JsonFieldDecoder visitVarBinary(String p, Object v) {
            return new JsonFieldDecoder(p) {
                @Override
                public final void decode(Decoded to, Object v) throws JSONException {
                    String encoded = null;
                    if (v != null) {
                        byte [] bytes = (byte[])v;
                        encoded = Encoder.base64Encode(bytes);
                    }
                    to.stringer.key(m_fieldName).value(encoded);
                }
            };
        }

        @Override
        public JsonFieldDecoder visitDecimal(String p, Object v) {
            return defaultDecoder(p);
        }
    };

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends RowDecoder.Builder {
        protected boolean m_camelCaseFieldNames = true;
        protected TimeZone m_timeZone = TimeZone.getDefault();
        protected String m_unspecifiedToken = "__VoltDB_unspecified__";
        protected List<String> m_partitionColumnNames = ImmutableList.of();
        protected HiveEndPointFactory m_endPointFactory = null;

        public Builder camelCaseFieldNames(boolean doit) {
            m_camelCaseFieldNames = doit;
            return this;
        }

        public Builder timeZone(TimeZone tz) {
            if (tz != null) {
                m_timeZone = tz;
            }
            return this;
        }

        public Builder partitionColumnNames(List<String> partitionColumnNames) {
            if (partitionColumnNames != null) {
                m_partitionColumnNames = ImmutableList.copyOf(partitionColumnNames);
            }
            return this;
        }

        public Builder unspecifiedToken(String unspecifiedToken) {
            if (unspecifiedToken != null && !unspecifiedToken.trim().isEmpty()) {
                m_unspecifiedToken = unspecifiedToken;
            }
            return this;
        }

        public Builder endPointFactory(HiveEndPointFactory endPointFactory) {
            m_endPointFactory = endPointFactory;
            return this;
        }

        public PartitionedJsonDecoder build() {
            List<String> partitionColumnNames = m_partitionColumnNames;

            if (m_camelCaseFieldNames) {
                partitionColumnNames = FluentIterable.from(partitionColumnNames)
                        .transform(camelCaseNameLowerFirst)
                        .toList();
            }
            return new PartitionedJsonDecoder(
                    m_camelCaseFieldNames,
                    partitionColumnNames,
                    m_firstFieldOffset,
                    m_timeZone,
                    m_unspecifiedToken,
                    m_endPointFactory);
        }
    }

    public static class DelegateBuilder extends RowDecoder.DelegateBuilder {
        private final Builder m_partitionedJsonBuilderDelegate;

        protected DelegateBuilder(Builder builder) {
            super(builder);
            m_partitionedJsonBuilderDelegate = builder;
        }

        protected DelegateBuilder(DelegateBuilder delegateBuilder) {
            super(delegateBuilder.getDelegateAs(Builder.class));
            m_partitionedJsonBuilderDelegate = delegateBuilder.getDelegateAs(Builder.class);
        }

        public DelegateBuilder camelCaseFieldNames(boolean doit) {
            m_partitionedJsonBuilderDelegate.camelCaseFieldNames(doit);
            return this;
        }

        public DelegateBuilder timeZone(TimeZone tz) {
            m_partitionedJsonBuilderDelegate.timeZone(tz);
            return this;
        }

        public DelegateBuilder partitionColumnNames(List<String> partitionColumnNames) {
            m_partitionedJsonBuilderDelegate.partitionColumnNames(partitionColumnNames);
            return this;
        }

        public DelegateBuilder unspecifiedToken(String unspecifiedToken) {
            m_partitionedJsonBuilderDelegate.unspecifiedToken(unspecifiedToken);
            return this;
        }

        public DelegateBuilder endPointFactory(HiveEndPointFactory endPointFactory) {
            m_partitionedJsonBuilderDelegate.endPointFactory(endPointFactory);
            return this;
        }

        @Override
        protected <TT extends RowDecoder.Builder> TT getDelegateAs(Class<TT> clazz) {
            return clazz.cast(m_partitionedJsonBuilderDelegate);
        }
    }
}
