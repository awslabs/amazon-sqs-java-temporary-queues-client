package com.amazonaws.services.sqs.proxy;

import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

public class JoiningSerializer implements InvertibleFunction<List<String>, String> {

    private final String delimiter;

    public JoiningSerializer(String delimiter) {
        this.delimiter = delimiter;
    }
    
    @Override
    public String apply(List<String> strings) {
        StringJoiner joiner = new StringJoiner(delimiter);
        strings.forEach(joiner::add);
        return joiner.toString();
    }

    @Override
    public List<String> unapply(String joined) {
        return Arrays.asList(joined.split(delimiter));
    }
}
