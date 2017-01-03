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

import java.util.concurrent.TimeUnit;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.EstTime;
import org.voltcore.utils.RateLimitedLogger;

public class HiveExportLogger {

    final static long SUPPRESS_INTERVAL = 10;
    final private VoltLogger m_logger = new VoltLogger("ExportClient");

    public HiveExportLogger() {
    }

    private void log(Level level, Throwable cause, String format, Object...args) {
        RateLimitedLogger.tryLogForMessage(
                EstTime.currentTimeMillis(),
                SUPPRESS_INTERVAL, TimeUnit.SECONDS,
                m_logger, level,
                cause, format, args
                );
    }

    public VoltLogger getLogger() {
        return m_logger;
    }

    public void trace(String format, Object...args) {
        if (m_logger.isTraceEnabled()) {
            log(Level.TRACE, null, format, args);
        }
    }

    public void debug(String format, Object...args) {
        if (m_logger.isDebugEnabled()) {
            log(Level.DEBUG, null, format, args);
        }
    }

    public void info(String format, Object...args) {
        if (m_logger.isInfoEnabled()) {
            log(Level.INFO, null, format, args);
        }
    }

    public void warn(String format, Object...args) {
        log(Level.WARN, null, format, args);
    }

    public void error(String format, Object...args) {
        log(Level.ERROR, null, format, args);
    }

    public void fatal(String format, Object...args) {
        log(Level.FATAL, null, format, args);
    }

    public void trace(String format, Throwable cause, Object...args) {
        if (m_logger.isTraceEnabled()) {
            log(Level.TRACE, cause, format, args);
        }
    }

    public void debug(String format, Throwable cause, Object...args) {
        if (m_logger.isDebugEnabled()) {
            log(Level.DEBUG, cause, format, args);
        }
    }

    public void info(String format, Throwable cause, Object...args) {
        if (m_logger.isInfoEnabled()) {
            log(Level.INFO, cause, format, args);
        }
    }

    public void warn(String format, Throwable cause, Object...args) {
        log(Level.WARN, cause, format, args);
    }

    public void error(String format, Throwable cause, Object...args) {
        log(Level.ERROR, cause, format, args);
    }

    public void fatal(String format, Throwable cause, Object...args) {
        log(Level.FATAL, cause, format, args);
    }
}
