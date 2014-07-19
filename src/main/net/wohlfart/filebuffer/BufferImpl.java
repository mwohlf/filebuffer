package net.wohlfart.filebuffer;


import java.nio.ByteBuffer;

public class BufferImpl implements IBuffer {

    private static final String DEFAULT_CACHE_DIR = "/tmp/" + BufferImpl.class.getName();
    private static final int DEFAULT_PAGE_SIZE = 512 * 1024;

    private long now = Long.MIN_VALUE;

    private final IPageHandler pageHandler;

    public BufferImpl() {
        pageHandler = new PageHandler(DEFAULT_CACHE_DIR, DEFAULT_PAGE_SIZE);
    }

    public void setPageSize(int size) {
        pageHandler.setPayloadSize(size);
    }

    public void setCacheDir(String cacheDir) {
        pageHandler.setCacheDir(cacheDir);
    }

    
    /**
     * pesist the ByteBuffer, this will modify the position in chunk
     */
    @Override
    public void enqueue(ByteBuffer chunk, long timestamp) throws CacheException {
        if (isPast(timestamp)) {
            throw new IllegalArgumentException(""
            		+ "the provided timestamp is too old: '" + timestamp + "'"
            		+ " can't change data in the past"
            		);
        }
        while (chunk.position() < chunk.limit()) {
            final PageImpl page = pageHandler.getLastPage(timestamp);
            page.write(chunk);
        }
    }

    @Override
    public void setStarttime(long timestamp) throws CacheException {
        // TODO: implement me
    }

    @Override
    public ByteBuffer dequeue() throws CacheException {
        return null;  // TODO: implement me
    }

    public byte[] dequeue(long timestamp) throws CacheException {
    	byte[] chunk = new byte[0];
        int offset = 0;
        while (offset < chunk.length) {
        	final PageImpl page = pageHandler.getFirstPage(timestamp);
       //     offset = page.read(offset, chunk);
        }
        return chunk;
    }

    
    public long getNow() {
        return now;
    }

    boolean isPast(long timestamp) {
        return timestamp < now;
    }

}
