package net.wohlfart.filebuffer;

import java.io.File;
import java.util.Set;

public interface IPageFactory {
	
    void setFilesize(int size);

	void setCacheDir(String cacheDir);

	IPage create(long timestamp);

	Set<IPage> getPages();

}
