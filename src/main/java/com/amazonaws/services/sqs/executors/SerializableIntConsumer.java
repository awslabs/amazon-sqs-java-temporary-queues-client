package com.amazonaws.services.sqs.executors;

import java.io.Serializable;
import java.util.function.IntConsumer;

public interface SerializableIntConsumer extends Serializable, IntConsumer {

}
