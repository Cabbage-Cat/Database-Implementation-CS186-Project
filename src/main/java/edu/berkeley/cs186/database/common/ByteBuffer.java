package edu.berkeley.cs186.database.common;

import java.nio.*;

/**
 * Wrapper around java.nio.ByteBuffer to implement our Buffer interface.
 */
public class ByteBuffer implements Buffer {
    private java.nio.ByteBuffer buf;

    private ByteBuffer(java.nio.ByteBuffer buf) {
        this.buf = buf;
    }

    public static Buffer allocateDirect(int capacity) {
        return new ByteBuffer(java.nio.ByteBuffer.allocateDirect(capacity));
    }

    public static Buffer allocate(int capacity) {
        return new ByteBuffer(java.nio.ByteBuffer.allocate(capacity));
    }

    public static Buffer wrap(byte[] array, int offset, int length) {
        return new ByteBuffer(java.nio.ByteBuffer.wrap(array, offset, length));
    }

    public static Buffer wrap(byte[] array) {
        return new ByteBuffer(java.nio.ByteBuffer.wrap(array));
    }

    @Override
    public Buffer slice() {
        return new ByteBuffer(buf.slice());
    }

    @Override
    public Buffer duplicate() {
        return new ByteBuffer(buf.duplicate());
    }

    @Override
    public byte get() {
        return buf.get();
    }

    @Override
    public Buffer put(byte b) {
        buf.put(b);
        return this;
    }

    @Override
    public byte get(int index) {
        return buf.get(index);
    }

    @Override
    public Buffer put(int index, byte b) {
        buf.put(index, b);
        return this;
    }

    @Override
    public Buffer get(byte[] dst, int offset, int length) {
        buf.get(dst, offset, length);
        return this;
    }

    @Override
    public Buffer get(byte[] dst) {
        buf.get(dst);
        return this;
    }

    public Buffer put(java.nio.ByteBuffer src) {
        buf.put(src);
        return this;
    }

    @Override
    public Buffer put(byte[] src, int offset, int length) {
        buf.put(src, offset, length);
        return this;
    }

    @Override
    public Buffer put(byte[] dst) {
        buf.put(dst);
        return this;
    }

    public boolean hasArray() {
        return buf.hasArray();
    }

    public byte[] array() {
        return buf.array();
    }

    public int arrayOffset() {
        return buf.arrayOffset();
    }

    public java.nio.ByteBuffer compact() {
        return buf.compact();
    }

    public boolean isDirect() {
        return buf.isDirect();
    }

    @Override
    public String toString() {
        return buf.toString();
    }

    @Override
    public int hashCode() {
        return buf.hashCode();
    }

    @Override
    public boolean equals(Object ob) {
        return buf.equals(ob);
    }

    public ByteOrder order() {
        return buf.order();
    }

    public Buffer order(ByteOrder bo) {
        buf.order(bo);
        return this;
    }

    @Override
    public char getChar() {
        return buf.getChar();
    }

    @Override
    public Buffer putChar(char value) {
        buf.putChar(value);
        return this;
    }

    @Override
    public char getChar(int index) {
        return buf.getChar(index);
    }

    @Override
    public Buffer putChar(int index, char value) {
        buf.putChar(index, value);
        return this;
    }

    @Override
    public short getShort() {
        return buf.getShort();
    }

    @Override
    public Buffer putShort(short value) {
        buf.putShort(value);
        return this;
    }

    @Override
    public short getShort(int index) {
        return buf.getShort(index);
    }

    @Override
    public Buffer putShort(int index, short value) {
        buf.putShort(index, value);
        return this;
    }

    @Override
    public int getInt() {
        return buf.getInt();
    }

    @Override
    public Buffer putInt(int value) {
        buf.putInt(value);
        return this;
    }

    @Override
    public int getInt(int index) {
        return buf.getInt(index);
    }

    @Override
    public Buffer putInt(int index, int value) {
        buf.putInt(index, value);
        return this;
    }

    @Override
    public long getLong() {
        return buf.getLong();
    }

    @Override
    public Buffer putLong(long value) {
        buf.putLong(value);
        return this;
    }

    @Override
    public long getLong(int index) {
        return buf.getLong(index);
    }

    @Override
    public Buffer putLong(int index, long value) {
        buf.putLong(index, value);
        return this;
    }

    @Override
    public float getFloat() {
        return buf.getFloat();
    }

    @Override
    public Buffer putFloat(float value) {
        buf.putFloat(value);
        return this;
    }

    @Override
    public float getFloat(int index) {
        return buf.getFloat(index);
    }

    @Override
    public Buffer putFloat(int index, float value) {
        buf.putFloat(index, value);
        return this;
    }

    @Override
    public double getDouble() {
        return buf.getDouble();
    }

    @Override
    public Buffer putDouble(double value) {
        buf.putDouble(value);
        return this;
    }

    @Override
    public double getDouble(int index) {
        return buf.getDouble(index);
    }

    @Override
    public Buffer putDouble(int index, double value) {
        buf.putDouble(index, value);
        return this;
    }

    public int capacity() {
        return buf.capacity();
    }

    public int limit() {
        return buf.limit();
    }

    public Buffer limit(int newLimit) {
        buf.limit(newLimit);
        return this;
    }

    public Buffer mark() {
        buf.mark();
        return this;
    }

    public Buffer reset() {
        buf.reset();
        return this;
    }

    public Buffer clear() {
        buf.clear();
        return this;
    }

    public Buffer flip() {
        buf.flip();
        return this;
    }

    public Buffer rewind() {
        buf.rewind();
        return this;
    }

    public int remaining() {
        return buf.remaining();
    }

    public boolean hasRemaining() {
        return buf.hasRemaining();
    }

    @Override
    public int position() {
        return buf.position();
    }

    @Override
    public Buffer position(int pos) {
        buf.position(pos);
        return this;
    }
}
