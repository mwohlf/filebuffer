package net.wohlfart.filebuffer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class SinglePageTest {

    private String filename;

    @Before
    public void prepare() throws IOException {
        filename = File.createTempFile(
                getClass().getCanonicalName(),
                String.valueOf(Thread.currentThread().getId())).getCanonicalPath();
        try {
            new File(filename).delete();
        } catch (Exception ex) {
            // ignore
        }
    }

    @After
    public void cleanup() {
        if (filename != null) {
            try {
                new File(filename).delete();
            } catch (Exception ex) {
                // ignore
            }
        }
    }

    @SuppressWarnings("resource")
	@Test
    public void smokeTest() throws IOException {
        PageImpl write = new PageImpl(filename).createFile(70);
        write.write(bb("blablablabla23"));
        write.close();

        PageImpl read = new PageImpl(filename).readOnly();
        assertBufferEquals(bb("blablablabla23"), read.read());
        read.close();
    }

    @SuppressWarnings("resource")
	@Test
    public void doubleWrite() throws IOException {
        PageImpl write = new PageImpl(filename).createFile(70);
        write.write(bb("test1data"));
        write.write(bb("2"));
        write.close();

        PageImpl read = new PageImpl(filename).readOnly();
        assertBufferEquals(bb("test1data"), read.read());
        assertBufferEquals(bb("2"), read.read());
        read.close();
    }
    
    @SuppressWarnings("resource")
    @Test
    public void splitWrite() throws IOException {
        PageImpl write = new PageImpl(filename).createFile(70);
        write.write(bb("testda2ta"));
        write.close();

        // data should be appended
        write = new PageImpl(filename).readWrite();
        write.write(bb("777"));
        write.close();

        PageImpl read = new PageImpl(filename).readOnly();
        assertBufferEquals(bb("testda2ta"), read.read());
        assertBufferEquals(bb("777"), read.read());

        // param should be consumed
        write = new PageImpl(filename).readWrite();
        ByteBuffer param = bb("tesdatad");
        assertEquals(8, param.remaining());
        assertEquals(0, param.position());
        assertEquals(8, param.capacity());
        assertEquals(8, param.limit());
        write.write(param);
        assertEquals(0, param.remaining());
        assertEquals(8, param.position());
        assertEquals(8, param.capacity());
        assertEquals(8, param.limit());

        // rewrite already written buffer shouldn't do anything
        write.write(param);
        assertEquals(0, param.remaining());
        
        write.close();
    }

    @SuppressWarnings("resource")
    @Test
    public void doubleWriteExeption() {
        PageImpl write2 = null;
        try {
            PageImpl write1 = new PageImpl(filename).createFile(70);
            write1.write(bb("testdata"));
            write1.close();

            write2 = new PageImpl(filename).createFile(70);
            fail();
        } catch (CacheException ex) {

        }
        assertNull(write2);
    }

    @Test
    public void sequentialReadWrite() throws IOException {
        PageImpl write = new PageImpl(filename).createFile(250);
        write.write(bb("hüzelbrützel"));
        write.close();

        PageImpl read = new PageImpl(filename).readOnly();
        assertEquals("hüzelbrützel", str(read.read()));
        read.close();

        write = new PageImpl(filename).readWrite();
        write.write(bb("maultaschen"));
        write.close();

        read = new PageImpl(filename).readOnly();
        assertEquals("hüzelbrützel", str(read.read()));
        assertEquals("maultaschen", str(read.read()));
        read.close();

        read = new PageImpl(filename).readOnly();
        assertEquals("hüzelbrützel", str(read.read()));
        assertEquals("maultaschen", str(read.read()));
        read.close();

        write = new PageImpl(filename).readWrite();
        write.write(bb("bretzelbrötchen"));
        write.close();

        read = new PageImpl(filename).readOnly();
        assertEquals("hüzelbrützel", str(read.read()));
        assertEquals("maultaschen", str(read.read()));
        assertEquals("bretzelbrötchen", str(read.read()));
    }


    @Test @Ignore
    public void incompleteWrite() throws IOException {
        // stuff in the file:
        // 8 byte index, 4 byte limit, (not part of the payload)
        // 4 byte chunksize in
        PageImpl write = new PageImpl(filename).createFile(10);
        ByteBuffer param = bb("1234567");
        write.write(param);
        assertEquals(7, param.remaining());

        param = bb("123456");
        write.write(param);
        assertEquals(0, param.remaining());

        PageImpl read = new PageImpl(filename).readOnly();
        assertBufferEquals(bb("123456"), read.read());
    }

    @Test @Ignore
    public void checkUnderflow() throws IOException {
        String filename = this.filename;
        ByteBuffer param;

        PageImpl write = new PageImpl(filename).createFile(5);
        param = bb("1");
        write.write(param);
        assertEquals(0, param.remaining());

        write.write(bb("2"));
        write.write(bb("3"));
        write.write(bb("4"));
        write.write(bb("5"));
    }

    @Test
    public void readEmpty() throws IOException, InterruptedException {
        PageImpl write = new PageImpl(filename).createFile(70);
        write.close();

        PageImpl read = new PageImpl(filename).readOnly();
        assertEquals("", str(read.read()));
        assertEquals("", str(read.read()));
    }




                /*
    @Test
    public void doubleRead() throws IOException {
        String filename = filename.getCanonicalPath();

        PageImpl write = new PageImpl(filename).createFile(70);
        write.write("testdatabla".getBytes());
        write.finalizeFile();

        PageImpl read = new PageImpl(filename).readWrite();
        assertEquals("testdatabla", new String(read.read()));

        // same data should be returned
        read = new PageImpl(filename).readWrite();
        assertEquals("testdatabla", new String(read.read()));
    }



    @Test
    public void doubleCreate() throws IOException, InterruptedException {
        String filename = filename.getCanonicalPath();

        try {
        	PageImpl page = new PageImpl(filename);
        	PageImpl write1 = page.createFile(70);
        	PageImpl write2 = page.createFile(70);
            fail("two write instances allowed");
        } catch (CacheException ex) {
            // expectd
        }
    }

    @Test
    public void concurrentReadWrite() throws IOException, InterruptedException {
        String filename = filename.getCanonicalPath();
        PageImpl write = new PageImpl(filename).createFile(1024);

        WriterThread writer = new WriterThread("abc:".getBytes(), 100, write);
        ReaderThread reader = new ReaderThread(filename);

        reader.start();
        writer.start();
        writer.join();

        String content = new String(new PageImpl(filename).readOnly().read());
        String[] c = content.split(":");
        assertEquals(100, c.length);
        for (String s : c) {
            assertEquals("abc", s);
        }
        reader.stop=true;
        reader.join();

        assertEquals(content, reader.result.toString());
    }
    */

    @SuppressWarnings("resource")
	@Test
    public void fanOutRead() throws IOException, InterruptedException {
        PageImpl write = new PageImpl(filename).createFile(1024);
        int iterations = 3;

        WriterThread writer = new WriterThread("bla:", iterations, write);

        ReaderThread reader1 = new ReaderThread(filename);
        ReaderThread reader2 = new ReaderThread(filename);
        ReaderThread reader3 = new ReaderThread(filename);
        ReaderThread reader4 = new ReaderThread(filename);

        reader1.start();
        writer.start();
        reader2.start();
        reader3.start();
        reader4.start();

        writer.join();
        reader1.writerrunning = false;
        reader2.writerrunning = false;
        reader3.writerrunning = false;
        reader4.writerrunning = false;     
        
        reader1.join();
        reader2.join();
        reader3.join();
        reader4.join();
        
        String content = "";
        String incoming = "";
        PageImpl reader = new PageImpl(filename).readOnly();
        do {
        	incoming = str(reader.read());
        	content += incoming;
        } while (incoming.length() > 0);
        String[] c = content.split(":");
        assertEquals(iterations, c.length);
        for (String s : c) {
            assertEquals("bla", s);
        }

        assertEquals(content, reader1.result.toString());
        assertEquals(content, reader2.result.toString());
        assertEquals(content, reader3.result.toString());
        assertEquals(content, reader4.result.toString());
    }




    class ReaderThread extends Thread {
        private final Random random = new Random();
        private String filename;
        private volatile StringBuilder result = new StringBuilder();
        private volatile boolean writerrunning= true;

        ReaderThread(String filename) {
            this.filename = filename;
        }

        @Override
        public void run() {
        	PageImpl source = new PageImpl(filename).readOnly();
            String incoming = "";
            do {
                try {
                    sleep(Math.abs(random.nextInt())%10);
                } catch (InterruptedException e) {
                    // ignore
                }
                incoming = str(source.read());
                result.append(incoming);
            } while (incoming.length() > 0 || writerrunning);
        }
    }

    class WriterThread extends Thread {
        private final Random random = new Random();
        private final String payload;
        private final int interations;
        private final PageImpl target;

        WriterThread(String payload, int interations, PageImpl target) {
            this.payload = payload;
            this.interations = interations;
            this.target = target;
        }

        @Override
        public void run() {
            for (int i = 0; i < interations; i++) {
                try {
                    sleep(Math.abs(random.nextInt())%10);
                } catch (InterruptedException e) {
                    // ignore
                }
                target.write(bb(payload));
            }
        }
    }


    // offset = pos = 0; cap = limit = size;
    ByteBuffer bb(String string) {
        ByteBuffer buffer = ByteBuffer.wrap(string.getBytes());
        buffer.rewind();
        return buffer;
    }
    
    String str(ByteBuffer buffer) {
    	int size = buffer.limit() - buffer.position();
    	byte[] bytes = new byte[size];
    	buffer.get(bytes, buffer.position(), buffer.limit());
    	return new String(bytes);
    }

    void assertBufferEquals(ByteBuffer expected, ByteBuffer actual) {
        assertEquals("buffer position is different", expected.position(), actual.position());
        assertEquals("buffer limit is different", expected.limit(), actual.limit());
        assertEquals("buffer capacity is different", expected.capacity(), actual.capacity());

        byte[] expContent = new byte[expected.capacity()];
        byte[] actualContent = new byte[actual.capacity()];

        expected.get(expContent);
        actual.get(actualContent);

        assertArrayEquals(expContent, actualContent);
    }


}