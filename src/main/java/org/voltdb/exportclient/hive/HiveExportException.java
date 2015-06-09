/*
 * The MIT License (MIT)
 *
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import java.util.Arrays;
import java.util.IllegalFormatConversionException;
import java.util.MissingFormatArgumentException;
import java.util.UnknownFormatConversionException;

public class HiveExportException extends RuntimeException {

    private static final long serialVersionUID = 4260074108700559787L;

    public HiveExportException() {
    }

    public HiveExportException(String format, Object...args) {
        super(format(format, args));
    }

    public HiveExportException(Throwable cause) {
        super(cause);
    }

    public HiveExportException(String format, Throwable cause, Object...args) {
        super(format(format, args), cause);
    }

    static protected String format(String format, Object...args) {
        String formatted = null;
        try {
            formatted = String.format(format, args);
        } catch (MissingFormatArgumentException|IllegalFormatConversionException|
                UnknownFormatConversionException ignoreThem) {
        }
        finally {
            if (formatted == null) {
                formatted = "Format: " + format + ", arguments: " + Arrays.toString(args);
            }
        }
        return formatted;
    }
}
