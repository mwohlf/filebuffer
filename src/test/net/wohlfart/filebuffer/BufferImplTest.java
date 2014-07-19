package net.wohlfart.filebuffer;


import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;


public class BufferImplTest {
    long now = 0;

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
    @Test @Ignore
    public void smokeTest() throws IOException {
        BufferImpl bufferImpl = new BufferImpl();
        bufferImpl.setPageSize(5);
        bufferImpl.setCacheDir(dir.getCanonicalPath());
        bufferImpl.enqueue(now, "hello world".getBytes());
        assertEquals("hello", new String(bufferImpl.dequeue(now, "hello".getBytes().length), "UTF-8"));
    }

    */
}
