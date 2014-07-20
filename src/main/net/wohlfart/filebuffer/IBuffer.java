package net.wohlfart.filebuffer;


import java.nio.ByteBuffer;

public interface IBuffer {

    // store data that happened at timestamp or later
    void enqueue(ByteBuffer chunk, long timestamp) throws CacheException;

	void setReadStart(long firstReadTimestamp) throws CacheException;

    // reads data including data at firstReadTimestamp
    ByteBuffer dequeue() throws CacheException;

}
