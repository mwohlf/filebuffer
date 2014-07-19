package net.wohlfart.filebuffer;


public interface IPageHandler {


    void setPayloadSize(int size);

	void setCacheDir(String cacheDir);

	
    // return the last page, use timestamp if we need to create a page
	PageImpl getLastPage(long timestamp);

    // get the first page that contains data for the timestamp
	PageImpl getFirstPage(long timestamp);

}
