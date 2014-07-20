package net.wohlfart.filebuffer;


public interface IPageHandler {

    void setFilesize(int size);

	void setCacheDir(String cacheDir);

	void setPageFactory(IPageFactory pageFactory);

	
	IPage getWritePage(long timestamp);

	void closeWritePage(IPage writePage);

	IPage getReadPage(long firstReadTimestamp);

	void closeReadPage(IPage readPage);

	IPage getNextReadPage(IPage readPage);

}
