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

import java.net.URI;
import java.util.List;

import org.apache.hive.hcatalog.streaming.HiveEndPoint;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.base.Predicate;
import com.google_voltpatches.common.collect.FluentIterable;
import com.google_voltpatches.common.collect.ImmutableList;

public class HiveEndPointFactory {
    final String m_uri;
    final String m_db;
    final String m_table;

    public HiveEndPointFactory(String uri, String db, String table) {
        Preconditions.checkArgument(
                uri != null && !uri.trim().isEmpty(),
                "uri is null or empty"
                );
        Preconditions.checkArgument(
                db != null && !db.trim().isEmpty(),
                "db is null or empty"
                );
        Preconditions.checkArgument(
                table != null && !table.trim().isEmpty(),
                "table is null or empty"
                );
        URI hUri = URI.create(uri);
        Preconditions.checkArgument(
                "thrift".equalsIgnoreCase(hUri.getScheme()),
                "unsupported URI scheme %s", hUri.getScheme()
                );
        m_uri = uri.intern();
        m_db = db.intern();
        m_table = table.intern();
    }

    public HiveEndPoint endPointFor(List<String> partitionVals) {
        checkPartitionValues(partitionVals);
        return new HiveEndPoint(m_uri, m_db, m_table, ImmutableList.copyOf(partitionVals));
    }

    public String getUri() {
        return m_uri;
    }

    public String getDb() {
        return m_db;
    }

    public String getTable() {
        return m_table;
    }

    @Override
    public String toString() {
        return "HiveEndPointFactory [uri=" + m_uri + ", db=" + m_db
                + ", table=" + m_table + "]";
    }

    final static Predicate<String> validPartitionValue = new Predicate<String>() {
        @Override
        public boolean apply(String input) {
            return input != null && !input.trim().isEmpty();
        }
    };

    public static void checkPartitionValues(List<String> partitionValues) {
        Preconditions.checkArgument(
                partitionValues != null
                && FluentIterable.from(partitionValues).allMatch(validPartitionValue),
                "partition values must be all non empty strings"
                );
    }
}
