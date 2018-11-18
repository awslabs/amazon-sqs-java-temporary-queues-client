package com.amazonaws.services.sqs.proxy;

import java.util.Base64;

public enum Base64Serializer implements InvertibleFunction<byte[], String>{
    
    INSTANCE {
        @Override
        public String apply(byte[] src) {
            return Base64.getEncoder().encodeToString(src);
        }

        @Override
        public byte[] unapply(String src) {
            return Base64.getDecoder().decode(src);
        }
    }
}
