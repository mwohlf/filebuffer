package net.wohlfart.filebuffer;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

public interface IBuffer {

    // store data that happened at timestamp or after
    void enqueue(ByteBuffer chunk, long timestamp) throws CacheException;

    // set the timestamp for the next dequeue operation
    void setStarttime(long timestamp) throws CacheException;

    // reads data including timestamp or before
    ByteBuffer dequeue() throws CacheException;

}
