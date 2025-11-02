package com.gdblab.graphstorage.storage;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Utils {

    public interface AutoCloseableIterable<T> extends Iterable<T>, AutoCloseable {
        @Override
        void close();

        default Stream<T> stream() {
            return StreamSupport.stream(spliterator(), false);
        }
    }

    public static class GraphStorageException extends RuntimeException {
        public GraphStorageException(String message, Throwable cause) {
            super(message, cause);
        }
    }
    
}
