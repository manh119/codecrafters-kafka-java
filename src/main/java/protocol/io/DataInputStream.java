package protocol.io;

import lombok.SneakyThrows;

import java.io.InputStream;
import java.nio.ByteBuffer;

public class DataInputStream implements DataInput {

    private final java.io.DataInputStream delegate;

    public DataInputStream(InputStream in) {
        this.delegate = new java.io.DataInputStream(in);
    }

    @SneakyThrows
    public ByteBuffer readNBytes(int n) {
        return ByteBuffer.wrap(delegate.readNBytes(n));
    }

    @SneakyThrows
    @Override
    public byte peekByte() {
        delegate.mark(1);
        byte value = delegate.readByte();
        delegate.reset();

        return value;
    }

    @SneakyThrows
    @Override
    public byte readSignedByte() {
        return delegate.readByte();
    }

    @SneakyThrows
    @Override
    public short readSignedShort() {
        return delegate.readShort();
    }

    @SneakyThrows
    @Override
    public int readSignedInt() {
        return delegate.readInt();
    }

    @SneakyThrows
    @Override
    public long readSignedLong() {
        return delegate.readLong();
    }
}
