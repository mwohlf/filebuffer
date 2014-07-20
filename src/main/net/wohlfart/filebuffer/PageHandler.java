package net.wohlfart.filebuffer;

import java.io.File;

import it.unimi.dsi.fastutil.longs.Long2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectSortedMap;

public class PageHandler implements IPageHandler {

	private int filesize;
	private File cacheDir;
	private IPageFactory pageFactory;
	
    // mapping the timestamp to the page, should contains all pages in the cache directory
    private final Long2ObjectSortedMap<IPage> pageCache = new Long2ObjectAVLTreeMap<>();


	@Override
	public void setFilesize(int size) {
		this.filesize = size;
	}

	@Override
	public void setCacheDir(String cacheDir) {
		this.cacheDir = new File(cacheDir);
	}
	
	@Override
	public void setPageFactory(IPageFactory pageFactory) {
		this.pageFactory = pageFactory;
	}

	public void init() {
		
	}
	
	@Override
	public IPage getWritePage(long timestamp) {
		if (pageCache.isEmpty()) {  // TODO: move this to init
			pageCache.put(timestamp, pageFactory.create(cacheDir, timestamp));
		}
		long last = pageCache.lastLongKey();
		if (last > timestamp) {
			throw new CacheException("data is out of order");
		}	
		IPage page = pageCache.get(last);
		if (!page.hasWriteBuffer()) {
			page.openWriteBuffer();
		}
		return page;
	}

	@Override
	public void closeWritePage(IPage writePage) {
		writePage.closeWriteBuffer();
	}

	@Override
	public IPage getReadPage(long firstReadTimestamp) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void closeReadPage(IPage readPage) {
		readPage.closeReadBuffer();
	}

	@Override
	public IPage getNextReadPage(IPage readPage) {
		long key = readPage.getTimestamp();
		// TODO
		return null;
	}

}
