package net.wohlfart.filebuffer;

import java.nio.ByteBuffer;

public interface IPage {

	long getTimestamp();
	
	long getIndex();
	
	
	// writing   ----------
	
	boolean hasWriteBuffer();

	void createWriteBuffer();

	void openWriteBuffer();

    int remainingForWrite();
	
    void write(ByteBuffer buffer);

	void closeWriteBuffer();

	
	// reading  --------

	boolean hasReadBuffer();

	void openReadBuffer();
	
	boolean isReadComplete();

    ByteBuffer read();
    
	void closeReadBuffer();
	
}
