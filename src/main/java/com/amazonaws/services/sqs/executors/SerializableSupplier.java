package com.amazonaws.services.sqs.executors;

import java.io.Serializable;
import java.util.function.Supplier;

public interface SerializableSupplier<V> extends Serializable, Supplier<V> {
    
}