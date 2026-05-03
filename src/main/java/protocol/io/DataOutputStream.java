package protocol.io;

import lombok.SneakyThrows;

import java.io.OutputStream;

public class DataOutputStream implements DataOutput, AutoCloseable {

    private final java.io.DataOutputStream delegate;

    public DataOutputStream(OutputStream outputStream) {
        this.delegate = new java.io.DataOutputStream(outputStream);
    }

    @SneakyThrows
    @Override
    public void writeRawBytes(byte[] bytes) {
        delegate.write(bytes);
    }

    @SneakyThrows
    @Override
    public void writeByte(byte value) {
        delegate.write(value);
    }

    @SneakyThrows
    @Override
    public void writeShort(short value) {
        delegate.writeShort(value);
    }

    @SneakyThrows
    @Override
    public void writeInt(int value) {
        delegate.writeInt(value);
    }

    @SneakyThrows
    @Override
    public void writeLong(long value) {
        delegate.writeLong(value);
    }

    @SneakyThrows
    @Override
    public void close() {
        delegate.close();
    }
}
