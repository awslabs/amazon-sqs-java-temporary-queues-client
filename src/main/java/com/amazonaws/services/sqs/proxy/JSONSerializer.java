package com.amazonaws.services.sqs.proxy;

import java.io.IOException;
import java.io.UncheckedIOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;

public class JSONSerializer<T> implements InvertibleFunction<T, String> {

	private final ObjectReader reader;
	private final ObjectWriter writer;
    
	public JSONSerializer(Class<T> klass) {
		this(klass, new ObjectMapper());
	}
	
	public JSONSerializer(Class<T> klass, ObjectMapper objectMapper) {
		this.reader = objectMapper.readerFor(klass);
		this.writer = objectMapper.writerFor(klass);
	}
	
	@Override
	public String apply(T object) {
		try {
			return writer.writeValueAsString(object);
		} catch (JsonProcessingException e) {
			throw new UncheckedIOException(e);
		}
	}

	@Override
	public T unapply(String serialized) {
		try {
			return reader.readValue(serialized);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

}
