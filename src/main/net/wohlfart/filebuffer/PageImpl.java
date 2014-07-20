package net.wohlfart.filebuffer;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.Buffer;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * a memory mapped file
 * 
 * 
 * multiple threads can read each gets their own view of the data
 * 
 * see: http://www.kdgregory.com/index.php?page=java.byteBuffer
 */
public class PageImpl implements IPage {

	public static final int LONG_SIZE = 8;
	public static final int INT_SIZE = 4;

	private static final int MIN_DATA_SIZE = INT_SIZE + INT_SIZE;  // would be: int[] {0, EOF}
	private static final int EOF = Integer.MIN_VALUE;  

	private final File cacheFile;

	private int fileSize;


	private ReadWriteLock lock = new ReentrantReadWriteLock();
	private Lock writeLock = lock.writeLock();
	private Lock readLock = lock.readLock();

	private volatile MappedByteBuffer writeBuffer;
	private ThreadLocal<ByteBuffer> readBuffer = new ThreadLocal<>(); // not sure if this is a good idea

	private PageMetadata metaData;


	public PageImpl(File file) {
		this.cacheFile = file;
	}

	public PageImpl(File file, int fileSize) {
		this.cacheFile = file;
		this.fileSize = fileSize;
	}

	@Override
	public long getTimestamp() {
		assert metaData != null
				: "need to call openWriteBuffer() or openReadBuffer() to get a timestamp";
		return metaData.getTimestamp();
	}

	@Override
	public long getIndex() {
		assert metaData != null
				: "need to call openWriteBuffer() or openReadBuffer() to get an index";
		return metaData.getIndex();
	}

	/**
	 * return true if this page is ready for writing
	 */
	@Override
	public boolean hasWriteBuffer() {
		try {
			writeLock.lock();
			return writeBuffer != null;
		} finally {
			writeLock.unlock();
		}
	}

	/**
	 * creates the page file with the specified file size and initializes the read and write buffers for this page
	 * the write buffer: 
	 *    position: the next free index which is after the metatdata
	 *    capacity: end of the file
	 *    limit: = end of the file
	 */
	@Override
	public void createWriteBuffer() {
		if (cacheFile.exists()) {
			throw new CacheException("file exists");
		}
		if (PageMetadata.METADATA_SIZE + MIN_DATA_SIZE > fileSize) {
			throw new CacheException("provided filesize is too small, for header and data we"
					+ " need at least " + (PageMetadata.METADATA_SIZE + MIN_DATA_SIZE)
					+ " but got " + fileSize);        	
		}

		writeLock.lock();
		try (RandomAccessFile rand = new RandomAccessFile(cacheFile, "rw");
				FileChannel channel = rand.getChannel()) {

			writeBuffer = channel.map(READ_WRITE, 0, fileSize);
			PageMetadata.writeInitialHeader(writeBuffer);
			metaData = new PageMetadata(writeBuffer);

		} catch (FileNotFoundException ex) {
			throw new CacheException("error finding file, file is '" + cacheFile + "'", ex);
		} catch (IOException ex) {
			throw new CacheException("error opening file", ex);
		} finally {
			writeLock.unlock();
		}
	}

	/**
	 * open write buffer for append
	 */
	@Override
	public void openWriteBuffer() {
		if (fileSize != cacheFile.length()) {
			throw new CacheException("filesize changed, was: " + fileSize + " now: " + cacheFile.length());
		}

		writeLock.lock();
		try (RandomAccessFile rand = new RandomAccessFile(cacheFile, "rw");
				FileChannel channel = rand.getChannel()) {

			writeBuffer = channel.map(READ_WRITE, 0, fileSize);
			metaData = new PageMetadata(writeBuffer);
			writeBuffer.position(PageMetadata.getLimit(writeBuffer)); // find the append position

		} catch (FileNotFoundException ex) {
			throw new CacheException("error finding file, file is '" + cacheFile + "'", ex);
		} catch (IOException ex) {
			throw new CacheException("error opening file", ex);
		} finally {
			writeLock.unlock();
		}
	}

	/**
	 * the byte count that can be stored in this buffer without getting an overflow
	 */
	@Override
	public int remainingForWrite() {

		try {
			writeLock.lock();
			if (writeBuffer == null) {
				throw new CacheException("no write buffer open");
			}
			return writeBuffer.remaining() - (INT_SIZE + INT_SIZE);
		} finally {
			writeLock.unlock();
		}
	}

	/**
	 * write as much data as possible into the buffer,
	 * incoming ByteBuffer is modified, the position is advanced
	 * to reflect how much has been written already
	 */
	public void write(ByteBuffer incoming) {
		
		try {
			writeLock.lock();
			int chunksize = incoming.limit() - incoming.position();
			// we need to add int for this chunk's offset plus the EOF marker for the read buffer
			if (remainingForWrite() < chunksize) { 
				writeBuffer.mark();
				writeBuffer.putInt(EOF);
				writeBuffer.reset();
				return;
			}
			writeBuffer.putInt(chunksize);
			writeBuffer.put(incoming);
			PageMetadata.setLimit(writeBuffer, writeBuffer.position());

		} finally {
			writeLock.unlock();
		}
	}

	/**
	 * flush and close the write buffer
	 */
	@Override
	public void closeWriteBuffer() {
		try {
			writeLock.lock();
			if (writeBuffer != null) {
				writeBuffer.force();
				destroyByteBuffer(writeBuffer);
				writeBuffer = null;
			}
		} finally {
			writeLock.unlock();
		}
	}


	/**
	 * return true if we can write into this page
	 */
	@Override
	public boolean hasReadBuffer() {
		try {
			readLock.lock();
			return readBuffer.get() != null;
		} finally {
			readLock.unlock();
		}		
	}

	/**
	 * initialize a thread load read buffer for this page
	 * the read buffer:
	 *    position: the next free index which is 0
	 *    capacity: end of the file
	 *    limit: = position	 
	 */
	public void openReadBuffer() {

		readLock.lock();
		try (RandomAccessFile rand = new RandomAccessFile(cacheFile, "r");
				FileChannel channel = rand.getChannel()) {

			MappedByteBuffer localReadBuffer = channel.map(READ_ONLY, 0, cacheFile.length());
			metaData = new PageMetadata(localReadBuffer);
			readBuffer.set(localReadBuffer.slice());

		} catch (FileNotFoundException ex) {
			throw new CacheException("error finding file, file: '" + cacheFile + "'", ex);
		} catch (IOException ex) {
			throw new CacheException("error opening file, file: '" + cacheFile + "'", ex);
		} finally {
			readLock.unlock();
		}
	}

	/**
	 * return true if all data have been read from this page
	 */
	@Override
	public boolean isReadComplete() {
		try {
			readLock.lock();
			
			ByteBuffer localReadBuffer = readBuffer.get();
			localReadBuffer.mark();
			int nextLimit = localReadBuffer.getInt();
			localReadBuffer.reset();

			return nextLimit == EOF;
		} finally {
			readLock.unlock();
		}		
	}

	/**
	 * read the next chunk of data from the thread local buffer
	 */
	public ByteBuffer read() {
		try {
			readLock.lock();
			final ByteBuffer localReadBuffer = readBuffer.get();
			if (localReadBuffer == null) {
				throw new CacheException("read buffer for the current thread is not initialized");
			}
			
			localReadBuffer.mark();

			// slice a chunk
			final int chunkSize = localReadBuffer.getInt();
			if (chunkSize == 0) {
				localReadBuffer.reset();
			} 
			localReadBuffer.limit(localReadBuffer.position() + chunkSize);
			final ByteBuffer result = localReadBuffer.slice();

			// prepare for the next read
			localReadBuffer.position(localReadBuffer.position() + chunkSize);
			localReadBuffer.limit(localReadBuffer.capacity());

			return result;
		} catch (BufferUnderflowException ex) {
			throw new CacheException("error reading data", ex);
		} catch (IllegalArgumentException ex) {
			throw new CacheException("error reading data, localReadBuffer is " + readBuffer.get(), ex);
		} finally {
			readLock.unlock();
		}
	}

	/**
	 * close the thread local read buffer
	 */
	@Override
	public void closeReadBuffer() {
		try {
			readLock.lock();
			ByteBuffer localReadBuffer = readBuffer.get();
			if (localReadBuffer != null) {
				destroyByteBuffer(localReadBuffer);
				readBuffer.remove();
			}
		} finally {
			readLock.unlock();
		}
	}

	// run the cleaner on the ByteBuffer, this seems to be common practice
	// however we might run into trouble when oracle changes the API
	// see: http://stackoverflow.com/questions/1854398/how-to-garbage-collect-a-direct-buffer-java
	private void destroyByteBuffer(Buffer buffer) {
		try {
			if (!buffer.isDirect()) {
				return;
			}
			Method cleanerMethod = buffer.getClass().getMethod("cleaner");
			cleanerMethod.setAccessible(true);
			Object cleaner = cleanerMethod.invoke(buffer);
			if (cleaner == null) {
				return;
			}
			Method cleanMethod = cleaner.getClass().getMethod("clean");
			cleanMethod.setAccessible(true);
			cleanMethod.invoke(cleaner);

		} catch (InvocationTargetException 
				| NoSuchMethodException 
				| SecurityException 
				| IllegalAccessException 
				| IllegalArgumentException ex) {
			throw new CacheException("ByteBuffer can't be destroyed" , ex);
		}
	}

}
