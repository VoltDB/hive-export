/*
 * This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
