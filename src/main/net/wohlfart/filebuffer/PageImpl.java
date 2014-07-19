package net.wohlfart.filebuffer;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * a memory mapped file that
 * see: http://www.kdgregory.com/index.php?page=java.byteBuffer
 */
public class PageImpl implements IPage, Closeable {

	public static final int LONG_SIZE = 8;
	public static final int INT_SIZE = 4;

	private static final int MIN_DATA_SIZE = 4;


	private long pageIndex; // the unique index for this page

	private final String filename; // the filename for this mapped buffer

	// size of the whole file including the header data
	private int fileSize;


	private ReadWriteLock lock = new ReentrantReadWriteLock();
	private volatile MappedByteBuffer writeBuffer;
	private ThreadLocal<ByteBuffer> readBuffer = new ThreadLocal<>(); // not sure if this is a good idea
	private Lock writeLock = lock.writeLock();
	private Lock readLock = lock.readLock();

	private volatile boolean dirty = false;
	private PageMetadata metaData;


	public PageImpl(String filename) {
		this.filename = filename;
	}

	/**
	 * creates the page file with the specified file size and initializes the read and write buffers for this page
	 * the write buffer: 
	 *    position: the next free index which is after the metatdata
	 *    capacity: end of the file
	 *    limit: = end of the file
	 * the read buffer:
	 *    position: the next free index which is 0 after the slice
	 *    capacity: end of the file (minus the header)
	 *    limit: = position 0 since there is nothing to read yet
	 */
	@Override
	public PageImpl createFile(int filesize) {
		this.fileSize = filesize;

		checkPreconditions();
		try (RandomAccessFile rand = new RandomAccessFile(filename, "rw");
				FileChannel channel = rand.getChannel()) {

			writeLock.lock();
			writeBuffer = channel.map(READ_WRITE, 0, fileSize);
			PageMetadata.writeHeader(writeBuffer);
			final ByteBuffer localReadBuffer = writeBuffer.slice().asReadOnlyBuffer();
			localReadBuffer.flip();
			readBuffer.set(localReadBuffer);
			dirty = false;

		} catch (FileNotFoundException ex) {
			throw new CacheException("error finding file, filename is '" + filename + "'", ex);
		} catch (IOException ex) {
			throw new CacheException("error opening file", ex);
		} finally {
			writeLock.unlock();
		}
		return this;
	}


	/**
	 * creates the page file with the specified file size and initializes the read and write buffers for this page
	 * the write buffer: 
	 *    position: the next free index which is after the metatdata
	 *    capacity: end of the file
	 *    limit: = end of the file
	 * the read buffer:
	 *    position: the next free index which is 0
	 *    capacity: end of the file
	 *    limit: = position
	 */
	public PageImpl readWrite() {

		try (RandomAccessFile file = new RandomAccessFile(filename, "rw");
				FileChannel channel = file.getChannel()) {

			writeLock.lock();
			if (writeBuffer == null) {
				writeBuffer = channel.map(READ_WRITE, 0, file.length());
				metaData = new PageMetadata(writeBuffer);
				if (metaData.getIndex() <= 0) {
					throw new CacheException("page index is " + metaData.getIndex() + " for '" + filename + "'");
				}
				writeBuffer.position(PageMetadata.getLimit(writeBuffer));
			}
			readBuffer.set(writeBuffer.slice().asReadOnlyBuffer());

		} catch (FileNotFoundException ex) {
			throw new CacheException("error finding file filename: '" + filename + "'", ex);
		} catch (IOException ex) {
			throw new CacheException("error opening file filename: '" + filename + "'", ex);
		} finally {
			writeLock.unlock();
		}
		return this;
	}

	/**
	 * initialize a thread load read buffer for this page
	 * the read buffer:
	 *    position: the next free index which is 0
	 *    capacity: end of the file
	 *    limit: = position	 
	 */
	public PageImpl readOnly() {

		try (RandomAccessFile file = new RandomAccessFile(filename, "r");
				FileChannel channel = file.getChannel()) {

			readLock.lock();
			MappedByteBuffer localReadBuffer = channel.map(READ_ONLY, 0, file.length());
			metaData = new PageMetadata(localReadBuffer);
			readBuffer.set(localReadBuffer.slice());

		} catch (FileNotFoundException ex) {
			throw new CacheException("error finding file, filename: '" + filename + "'", ex);
		} catch (IOException ex) {
			throw new CacheException("error opening file, filename: '" + filename + "'", ex);
		} finally {
			readLock.unlock();
		}
		return this;
	}

	// write as much data as possible into the buffer,
	// incoming ByteBuffer is modified, the position is advanced
	// to reflect how much has been written already
	public void write(ByteBuffer incoming) {
		try {
			writeLock.lock();

			int chunksize = incoming.limit() - incoming.position();
			if (writeBuffer.remaining() < chunksize + LONG_SIZE) {
				return;
			}
			writeBuffer.putInt(chunksize);
			writeBuffer.put(incoming);
			PageMetadata.setLimit(writeBuffer, writeBuffer.position());
			dirty = true;

		} finally {
			writeLock.unlock();
		}
	}

	// read the next chunk
	public ByteBuffer read() {
		try {
			readLock.lock();
			final ByteBuffer localReadBuffer = readBuffer.get();
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

	public int remaining() {
		return writeBuffer.remaining();
	}

	private void checkPreconditions() {
		if (new File(filename).exists()) {
			throw new CacheException("file for new page already exists "
					+ " filename is '" + filename + "'");
		}
		if (writeBuffer != null) {
			throw new CacheException("write buffer already created for " + filename);
		}
		if (PageMetadata.METADATA_SIZE + MIN_DATA_SIZE > fileSize) {
			throw new CacheException("provided filesize is too small, for header and data we"
					+ " need at least " + (PageMetadata.METADATA_SIZE + MIN_DATA_SIZE)
					+ " but got " + fileSize);        	
		}
	}

	/**
	 * closes the write buffer
	 */
	@Override
	public void finalizeFile() {
		try {
			writeLock.lock();
			writeBuffer.force();
			dirty = false;
		} finally {
			writeLock.unlock();
		}
	}

	@Override
	public void close() {
		if (writeBuffer != null) {
			writeBuffer.force();
		}
		dirty = false;
		metaData = null;
	}

}
