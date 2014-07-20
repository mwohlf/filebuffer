package net.wohlfart.filebuffer;


public interface IPageHandler {


    void setFilesize(int size);

	void setCacheDir(String cacheDir);

	
    // return the last page, use timestamp if we need to create a page
	IPage getLastPage(long timestamp);

    // get the first page that contains data for the timestamp
	IPage getFirstPage(long timestamp);

	IPage getWritePage(long timestamp);

	void closeWritePage(IPage writePage);

	IPage getReadPage(long firstReadTimestamp);

	void closeReadPage(IPage readPage);

	IPage getNextReadPage(IPage readPage);

}
