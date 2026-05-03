package protocol.io;

import lombok.SneakyThrows;

import java.nio.ByteBuffer;

public class DataByteBuffer implements DataInput {

    private final ByteBuffer delegate;

    public DataByteBuffer(ByteBuffer buffer) {
        this.delegate = buffer;
    }

    @SneakyThrows
    public ByteBuffer readNBytes(int n) {
        final var bytes = new byte[n];

        delegate.get(bytes, 0, bytes.length);

        return ByteBuffer.wrap(bytes);
    }

    @Override
    public byte peekByte() {
        delegate.mark();
        byte value = delegate.get();
        delegate.reset();

        return value;
    }

    @SneakyThrows
    @Override
    public byte readSignedByte() {
        return delegate.get();
    }

    @SneakyThrows
    @Override
    public short readSignedShort() {
        return delegate.getShort();
    }

    @SneakyThrows
    @Override
    public int readSignedInt() {
        return delegate.getInt();
    }

    @SneakyThrows
    @Override
    public long readSignedLong() {
        return delegate.getLong();
    }
}
