package net.wohlfart.filebuffer;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import it.unimi.dsi.fastutil.longs.Long2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectSortedMap;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class PageHandler implements IPageHandler {

    private static final int DEFAULT_PAYLOAD_SIZE = 1024 * 500;
    private static final String FILENAME_FORMAT = "yyyy.MM.dd-HH:mm:ss-SSS"; // we always use UTC
    private static final String FILENAME_POSTFIX = ".data"; // always use UTC
    private static final Pattern FILENAME_PATTERN = Pattern.compile("(" + FILENAME_FORMAT + ")" + FILENAME_POSTFIX);
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormat.forPattern(FILENAME_FORMAT).withZoneUTC();


    private final Long2ObjectSortedMap<PageImpl> pageCache = new Long2ObjectAVLTreeMap<>();

    private File cacheDir;

    private int payloadSize = DEFAULT_PAYLOAD_SIZE;

    public PageHandler(String cacheDirName, int payloadSize) {
		this.cacheDir = validateDir(cacheDirName);
        this.payloadSize = payloadSize;
        initializeCacheDir();
	}

    @Override
    public void setPayloadSize(int size) {
        payloadSize = size;
    }

    @Override
    public void setCacheDir(String dirname) {
		this.cacheDir = validateDir(dirname);
        initializeCacheDir();
    }

    // return a page for appending make sure there is some room left to write in the returned page
    @Override
    public PageImpl getLastPage(long timestamp) {
        long ts = timestamp;
        if (pageCache.isEmpty()) {
            pageCache.put(ts, createPage(ts));
        }
        PageImpl page = pageCache.get(pageCache.lastLongKey());
        while (page.remaining() == 0) {
            ts++;
            pageCache.put(ts, createPage(ts));
            page = pageCache.get(pageCache.lastLongKey());
        }
        return page;
    }
    
    // return a page for reading
    @Override 
    public PageImpl getFirstPage(long from) {
        if (pageCache.isEmpty()) {
            pageCache.put(from, createPage(from));
        }
        final Long2ObjectSortedMap<PageImpl> tail = pageCache.tailMap(from);
        Long key = tail.firstKey();
        return tail.get(key);
    }


    private void initializeCacheDir() {
    	pageCache.clear();
        File[] files = cacheDir.listFiles();
        if (files == null) {
            throw new IllegalArgumentException("can't read directory: '" + cacheDir + "'");
        }
        for (File file : files) {
            if (isPage(file)) {
                final String filename = file.getName();
                pageCache.put(parseDate(filename), new PageImpl(filename));
            }
        }
    }

    private File validateDir(String cacheDir) {
        final File result = new File(cacheDir);
        if (!result.exists() && !result.mkdirs()) {
            throw new IllegalArgumentException("can't create directory at: '" + cacheDir + "'");
        }
        if (!result.canWrite()) {
            throw new IllegalArgumentException("can't write to: '" + cacheDir + "'");
        }
        return result;
    }

    private PageImpl createPage(long instant) {
        return new PageImpl(cacheDir.getPath() + "/" + DATE_FORMAT.print(instant) + FILENAME_POSTFIX).createFile(payloadSize);
    }

    private boolean isPage(File file) {
        return file.exists()
            && file.canRead()
            && !file.isDirectory()
            && !file.isHidden()
            && (parseDate(file.getName()) > Long.MIN_VALUE);
    }

    private long parseDate(String filename) {
        Matcher matcher = FILENAME_PATTERN.matcher(filename);
        if (!matcher.matches()) {
            return Long.MIN_VALUE;
        }
        try {
            DateTime date = DateTime.parse(
                    matcher.group(1), DATE_FORMAT);
            return date.getMillis();
        } catch (IllegalArgumentException ex) {
            return Long.MIN_VALUE;
        }
    }

}
