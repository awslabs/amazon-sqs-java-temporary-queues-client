package com.amazonaws.services.sqs.executors;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;

public enum DefaultSerializer implements InvertibleFunction<Object, byte[]> {

	INSTANCE {
		@Override
		public byte[] apply(Object object) {
			try {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				ObjectOutputStream out = new ObjectOutputStream(baos);
				out.writeObject((Serializable)object);
				return baos.toByteArray();
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}

		@Override
		public Object unapply(byte[] serialized) {
			try {
				ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
				ObjectInputStream in = new ObjectInputStream(bais);
				return in.readObject();
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			} catch (ClassNotFoundException e) {
				// TODO-RS: Better exception type. NoClassDefFoundError is too drastic.
				throw new RuntimeException(e);
			}
		}
	};
	
	public static <T> InvertibleFunction<T, byte[]> forClass(Class<T> klass) {
		// TODO-RS: Should be able to use InvertibleFunction.compose or andThen
		return InvertibleFunction.of(INSTANCE::apply, s -> klass.cast(INSTANCE.unapply(s)));
	}
}
