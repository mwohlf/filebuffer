package net.wohlfart.filebuffer;

import java.nio.ByteBuffer;

public interface IPage {

	/**
	 * creates a page file and initializes read and write buffers
	 */
	PageImpl createFile(int payloadSize);

	/**
	 * close the write buffer (you can still read)
	 */
	void finalizeFile();

    /**
     * close any buffer
     */
	void close();

    /**
     * read data from the current position of this page
     */
    ByteBuffer read();

    /**
     * write the buffer into the page file, the position of the buffer will be modified
     */
    void write(ByteBuffer buffer);
	
}
