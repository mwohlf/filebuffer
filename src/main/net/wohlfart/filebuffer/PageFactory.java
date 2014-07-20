package net.wohlfart.filebuffer;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class PageFactory implements IPageFactory {
	
    private static final int DEFAULT_FILE_SIZE = 1024 * 500;
    private static final String TIMESTAMP_FORMAT = "yyyy.MM.dd-HH:mm:ss-SSS-z"; // we always use UTC
    private static final String FILENAME_POSTFIX = ".page";
    private static final Pattern FILENAME_PATTERN = Pattern.compile("(" + TIMESTAMP_FORMAT + ")" + FILENAME_POSTFIX);
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormat.forPattern(TIMESTAMP_FORMAT).withZoneUTC();

	
	private int filesize = DEFAULT_FILE_SIZE;
	private File cacheDir = new File("/tmp");

	@Override
	public void setFilesize(int size) {
		this.filesize = size;
	}

	@Override
	public void setCacheDir(String cacheDir) {
		this.cacheDir = new File(cacheDir);
	}
		
	@Override
	public IPage create(long timestamp) {
		File file = new File(cacheDir, String.valueOf(timestamp));
		return new PageImpl(file, filesize);
	}

	@Override
	public Set<IPage> getPages() {
		Set<IPage> result = new HashSet<>();
		File[] files = cacheDir.listFiles();
		for (File file : files) {
			result.add(new PageImpl(file));
		}
		return result;
	}

}
