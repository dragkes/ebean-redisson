package io.ebean.redisson.encode;

public interface Encode {

    byte[] encode(Object value);

    Object decode(byte[] data);
}
