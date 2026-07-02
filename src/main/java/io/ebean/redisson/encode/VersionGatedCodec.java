package io.ebean.redisson.encode;

import io.ebeaninternal.server.cache.CachedBeanData;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.redisson.client.codec.BaseCodec;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;

/**
 * Wraps a bean codec and stores the entity {@code @Version} as a fixed big-endian prefix in front of the
 * encoded value, behind a 2-byte magic {@link #MARKER}. The layout is {@code [marker][8-byte version][bean]}.
 * This lets a server-side Lua script read and compare versions (the 8 bytes after the marker, which sort
 * lexicographically the same as numerically for non-negative longs) without deserialising the bean -
 * enabling an atomic, single round-trip version-gated put in {@code RedissonCache}.
 * <p>
 * <b>Backward compatible.</b> Values written before this codec existed (or by any other writer) have no
 * marker - for the delegate bean codec they are Java-serialised and so begin with the stream magic
 * {@code 0xAC 0xED ...}, which never collides with {@link #MARKER}. {@link #getValueDecoder()} therefore
 * only strips the prefix when the marker is present and otherwise decodes the legacy bytes unchanged, so
 * a region that already holds un-prefixed entries keeps reading correctly and is upgraded to the prefixed
 * format on the next write. The companion Lua CAS treats a marker-less stored value as having no comparable
 * version and overwrites it (a versioned write is authoritative over an unversioned legacy entry).
 * <p>
 * The prefix is otherwise transparent to readers: remove/clear/trim need no special handling (the prefix
 * is part of the stored value), and {@code RMapCacheNative.get}/{@code getAll} return the bean unchanged.
 */
public class VersionGatedCodec extends BaseCodec {
    public static final byte[] MARKER = {(byte) 0xEB, (byte) 0x01};
    public static final int VERSION_BYTES = 8;
    public static final int PREFIX_BYTES = 2 + VERSION_BYTES;

    private final CacheCodec delegate;
    private final Encoder valueEncoder;
    private final Decoder<Object> valueDecoder;

    public VersionGatedCodec(CacheCodec delegate) {
        this.delegate = delegate;
        Encoder delegateEncoder = delegate.getValueEncoder();
        Decoder<Object> delegateDecoder = delegate.getValueDecoder();

        this.valueEncoder = in -> {
            long version = (in instanceof CachedBeanData) ? ((CachedBeanData) in).getVersion() : 0L;
            ByteBuf inner = delegateEncoder.encode(in);
            try {
                ByteBuf out = ByteBufAllocator.DEFAULT.buffer(PREFIX_BYTES + inner.readableBytes());
                out.writeBytes(MARKER);
                out.writeLong(version);
                out.writeBytes(inner);
                return out;
            } finally {
                inner.release();
            }
        };

        this.valueDecoder = (buf, state) -> {
            if (hasMarker(buf)) {
                buf.skipBytes(PREFIX_BYTES);
            }
            return delegateDecoder.decode(buf, state);
        };
    }

    private static boolean hasMarker(ByteBuf buf) {
        int ri = buf.readerIndex();
        if (buf.readableBytes() < PREFIX_BYTES) {
            return false;
        }
        for (int i = 0; i < MARKER.length; i++) {
            if (buf.getByte(ri + i) != MARKER[i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Encoder getValueEncoder() {
        return valueEncoder;
    }

    @Override
    public Decoder<Object> getValueDecoder() {
        return valueDecoder;
    }

    @Override
    public Encoder getMapKeyEncoder() {
        return delegate.getMapKeyEncoder();
    }

    @Override
    public Decoder<Object> getMapKeyDecoder() {
        return delegate.getMapKeyDecoder();
    }
}
