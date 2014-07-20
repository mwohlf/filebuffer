package net.wohlfart.filebuffer;


import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

public class BufferImpl implements IBuffer, Closeable {

    private IPageHandler pageHandler;
    
	private long firstReadTimestamp;
	
	IPage readPage;
	IPage writePage;

    public void setPageHandler(IPageHandler pageHandler) {
        this.pageHandler = pageHandler;
    }

    /**
     * persist the ByteBuffer, this will modify the position in chunk
     */
    @Override
    public void enqueue(ByteBuffer chunk, long timestamp) throws CacheException {
    	if (writePage == null) {
    		writePage = pageHandler.getWritePage(timestamp);
        	assert writePage.remaining() > (chunk.limit() - chunk.position()) : "chunk is too big for new page";
    	}
    	if (writePage.remaining() < (chunk.limit() - chunk.position())) {
    		pageHandler.closeWritePage(writePage);
    		writePage = pageHandler.getWritePage(timestamp);
        	assert writePage.remaining() > (chunk.limit() - chunk.position()) : "chunk is too big for new page";
    	}
    	writePage.write(chunk);
    }

    /**
	 * set the read pointer to timestamp or shortly before
     */
    @Override
    public void setReadStart(long firstReadTimestamp) throws CacheException {
        this.firstReadTimestamp = firstReadTimestamp; 
    }

    @Override
    public ByteBuffer dequeue() throws CacheException {
    	if (readPage == null) {
    		readPage = pageHandler.getReadPage(firstReadTimestamp);
    	}
    	if (readPage.isFullyRead()) {
    		pageHandler.closeReadPage(readPage);
    		readPage = pageHandler.getNextReadPage(readPage);
    	}
    	return readPage.read();
    }

	@Override
	public void close() throws IOException {
    	if (readPage != null) {
    		pageHandler.closeReadPage(readPage);
    	}
    	if (writePage != null) {
    		pageHandler.closeWritePage(writePage);
    	}
		
	}

}
