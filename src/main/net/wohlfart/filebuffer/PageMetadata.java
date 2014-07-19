package net.wohlfart.filebuffer;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

class PageMetadata {
	
    private static volatile long nextPageIndex = 1; // should be kept as instance in the handler

    private static final int INDEX_POS = 0;
    private static final int LIMIT_POS = 8;
    private static final int HEADER_OFFSET = PageImpl.LONG_SIZE + PageImpl.LONG_SIZE;

    private final long bufferIndex;

	static final int METADATA_SIZE = PageImpl.LONG_SIZE + PageImpl.INT_SIZE; 
    
	
	static void setLimit(ByteBuffer writeBuffer, long limit) {
        writeBuffer.putLong(LIMIT_POS, limit);		
	}
	
	static int getLimit(ByteBuffer writeBuffer) {
		return (int) writeBuffer.getLong(LIMIT_POS);
	}
	
	
    /**
     * initializes the write buffer with the header information and sets the position of the 
     * buffer just after the header
     */
	static void writeHeader(MappedByteBuffer writeBuffer) {
        writeBuffer.putLong(INDEX_POS, nextPageIndex++);
        writeBuffer.putLong(LIMIT_POS, HEADER_OFFSET);
        writeBuffer.position(HEADER_OFFSET);
	}

	/**
	 * create a metatdata, after returning the buffers position is after the header
	 */
	PageMetadata(ByteBuffer buffer) {
		bufferIndex = buffer.getLong(INDEX_POS);
		buffer.position(HEADER_OFFSET);
	}
    
	long getIndex() {
		return bufferIndex;
	}


}
