package net.wohlfart.filebuffer;

import java.io.File;

public interface IPageFactory {
	
	IPage create(File dir, long timestamp);

}
