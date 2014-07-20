package net.wohlfart.filebuffer;

import java.io.File;

public class PageFactory implements IPageFactory {
	
	
	public IPage create(File dir, long timestamp) {
		return new PageImpl();
	}

}
