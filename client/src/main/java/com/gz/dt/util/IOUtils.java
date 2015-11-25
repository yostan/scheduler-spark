package com.gz.dt.util;

import java.io.*;

/**
 * Created by naonao on 2015/10/27.
 */
public abstract class IOUtils {

    public static void delete(File file) throws IOException {
        ParamChecker.notNull(file, "file");
        if (file.getAbsolutePath().length() < 5) {
            throw new RuntimeException(XLog.format("Path[{0}] is too short, not deleting", file.getAbsolutePath()));
        }
        if (file.exists()) {
            if (file.isDirectory()) {
                File[] children = file.listFiles();
                if (children != null) {
                    for (File child : children) {
                        delete(child);
                    }
                }
            }
            if (!file.delete()) {
                throw new RuntimeException(XLog.format("Could not delete path[{0}]", file.getAbsolutePath()));
            }
        }
    }


    public static String getReaderAsString(Reader reader, int maxLen) throws IOException {
        ParamChecker.notNull(reader, "reader");
        StringBuffer sb = new StringBuffer();
        char[] buffer = new char[2048];
        int read;
        int count = 0;
        while ((read = reader.read(buffer)) > -1) {
            count += read;
            if (maxLen > -1 && count > maxLen) {
                throw new IllegalArgumentException(XLog.format("stream exceeds limit [{0}]", maxLen));
            }
            sb.append(buffer, 0, read);
        }
        reader.close();
        return sb.toString();
    }


    public static void copyCharStream(Reader reader, Writer writer) throws IOException {
        ParamChecker.notNull(reader, "reader");
        ParamChecker.notNull(writer, "writer");
        char[] buffer = new char[4096];
        int read;
        while ((read = reader.read(buffer)) > -1) {
            writer.write(buffer, 0, read);
        }
        writer.close();
        reader.close();
    }


    public static InputStream getResourceAsStream(String path, int maxLen) throws IOException {
        ParamChecker.notEmpty(path, "path");
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
        if (is == null) {
            throw new IllegalArgumentException(XLog.format("resource [{0}] not found", path));
        }
        return is;
    }


}
