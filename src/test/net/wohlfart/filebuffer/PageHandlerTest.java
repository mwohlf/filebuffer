package net.wohlfart.filebuffer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;


public class PageHandlerTest {

    private File dir;

    @Before
    public void prepare() throws IOException {
        dir = File.createTempFile(
                getClass().getCanonicalName(),
                String.valueOf(Thread.currentThread().getId()));
        String dirname = dir.getPath() + "Dir";
        dir.delete();
        dir = new File(dirname);
        dir.mkdir();
    }

    @After
    public void cleanup() {
        if (dir != null) {
            try {
                dir.delete();
            } catch (Exception ex) {
                // ignore
            }
        }
    }

    /*

    @Test
    public void smokeTest() {
        PageHandler pageHandler = new PageHandler(dir.getAbsolutePath(), 24);
        PageImpl writer = pageHandler.getLastPage(0);
        writer.write("hello world".getBytes());
        PageImpl reader = pageHandler.getFirstPage(0);
        assertEquals("hello world", new String(reader.read()));
    }

    @Test
    public void pageChange() {
        PageHandler pageHandler = new PageHandler(dir.getAbsolutePath(), 6);
        PageImpl writer = pageHandler.getLastPage(0);
        writer.write("hello world".getBytes());
        PageImpl reader = pageHandler.getFirstPage(0);
        assertEquals("hello ", new String(reader.read()));
    }

    */
}
