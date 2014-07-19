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
        IPage write = new PageImpl(filename).createFile(PageMetadata.METADATA_SIZE + 70);
        write.write(bb("blablablabla23"));
        write.close();

        IPage read = new PageImpl(filename).readOnly();
        assertBufferEquals(bb("blablablabla23"), read.read());
        read.close();
    }

    @SuppressWarnings("resource")
	@Test
    public void doubleWrite() throws IOException {
    	IPage write = new PageImpl(filename).createFile(70);
        write.write(bb("test1data"));
        write.write(bb("2"));
        write.close();

        IPage read = new PageImpl(filename).readOnly();
        assertBufferEquals(bb("test1data"), read.read());
        assertBufferEquals(bb("2"), read.read());
        read.close();
    }
    
    @SuppressWarnings("resource")
    @Test
    public void doubleRead() throws IOException {
    	IPage write = new PageImpl(filename).createFile(PageMetadata.METADATA_SIZE + 70);
        write.write(bb("testdatabla"));
        write.close();

        IPage read = new PageImpl(filename).readWrite();
        assertEquals("testdatabla", str(read.read()));
        read.close();

        // same data should be returned
        read = new PageImpl(filename).readWrite();
        assertEquals("testdatabla", str(read.read()));
        read.close();
    }

    @SuppressWarnings("resource")
    @Test
    public void splitWrite() throws IOException {
    	IPage write = new PageImpl(filename).createFile(PageMetadata.METADATA_SIZE + 70);
        write.write(bb("testda2ta"));
        write.close();

        // data should be appended
        write = new PageImpl(filename).readWrite();
        write.write(bb("777"));
        write.close();

        IPage read = new PageImpl(filename).readOnly();
        assertBufferEquals(bb("testda2ta"), read.read());
        assertBufferEquals(bb("777"), read.read());
        read.close();

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
    	IPage write2 = null;
        try {
            PageImpl write1 = new PageImpl(filename).createFile(PageMetadata.METADATA_SIZE + 70);
            write1.write(bb("testdata"));
            write1.close();

            write2 = new PageImpl(filename).createFile(70);
            fail();
        } catch (CacheException ex) {

        }
        assertNull(write2);
    }

    @SuppressWarnings("resource")
    @Test
    public void sequentialReadWrite() throws IOException {
    	IPage write = new PageImpl(filename).createFile(PageMetadata.METADATA_SIZE + 250);
        write.write(bb("hüzelbrützel"));
        write.close();

        IPage read = new PageImpl(filename).readOnly();
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

    @SuppressWarnings("resource")
    @Test
    public void writeIntoFullPage() throws IOException {
        // stuff in the file:
        // 8 byte index, 4 byte limit, (not part of the payload)
        // 4 byte chunksize in
    	IPage write = new PageImpl(filename).createFile(PageMetadata.METADATA_SIZE + 19); 
        ByteBuffer param = bb("12345678901234567890123456789012345");
        write.write(param);
        assertEquals(35, param.remaining());

        // 35 minus HEADER_SIZE (16) --> we have 19 to write
        String string = "12345678901234567890123456789012345".substring(PageMetadata.METADATA_SIZE);
        param = bb(string);
        write.write(param);
        assertEquals(string.length(), param.remaining());
        
        // we need 4 for the limit pointer  --> 15
        string = "12345678901234567890123456789012345".substring(PageMetadata.METADATA_SIZE + PageImpl.INT_SIZE);
        param = bb(string);
        write.write(param);
        assertEquals(string.length(), param.remaining());

        // we need another 4 for the EOF pointer --> 11
        string = "12345678901234567890123456789012345".substring(PageMetadata.METADATA_SIZE + PageImpl.INT_SIZE  + PageImpl.INT_SIZE);
        param = bb(string);
        write.write(param);
        assertEquals(0, param.remaining());
       
        // no more room to write
        String s = ".";
        param = bb(s);
        write.write(param);
        assertEquals(s.toCharArray().length, param.remaining());

        // check if we can read the stuff again
        IPage read = new PageImpl(filename).readOnly();
        assertBufferEquals(bb(string), read.read());
              
        // stil no more room to write?
        String t = ".";
        param = bb(t);
        write.write(param);
        assertEquals(t.toCharArray().length, param.remaining());

    }

    @SuppressWarnings("resource")
    @Test
    public void checkUnderflow() throws IOException {
        ByteBuffer param;

        IPage write = new PageImpl(filename).createFile(PageMetadata.METADATA_SIZE + 20);
        
        write.write(param = bb("1"));
        assertEquals(0, param.remaining());  // 15 = 20 - (4+1)
        
        write.write(param = bb("2"));
        assertEquals(0, param.remaining());  // 10 = 15 - (4+1)

        write.write(param = bb("3"));
        assertEquals(0, param.remaining());  // 5 = 10 - (4+1)

        write.write(param = bb("4"));
        assertEquals(1, param.remaining());  // 5 left but we need 4 for the EOF --> full

        write.write(param = bb("5"));
        assertEquals(1, param.remaining());

    }

    @Test
    public void readEmpty() throws IOException, InterruptedException {
    	IPage write = new PageImpl(filename).createFile(PageMetadata.METADATA_SIZE + 70);
        write.close();

        IPage read = new PageImpl(filename).readOnly();
        assertEquals("", str(read.read()));
        assertEquals("", str(read.read()));
    }


    @Test
    public void doubleCreate() throws IOException, InterruptedException {
        try {
        	IPage page = new PageImpl(filename);
        	IPage write1 = page.createFile(70);
        	IPage write2 = page.createFile(70);
            fail("two write instances allowed");
        } catch (CacheException ex) {
            // expectd
        }
    }

    @Test
    public void concurrentReadWrite() throws IOException, InterruptedException {
    	IPage write = new PageImpl(filename).createFile(1024);

        WriterThread writer = new WriterThread("abc:", 100, write);
        ReaderThread reader = new ReaderThread(filename);

        reader.start();
        writer.start();
        writer.join();
        reader.writerrunning = false;

        String content = "";
        String incoming = "";
        PageImpl read = new PageImpl(filename).readOnly();
        do {
        	incoming = str(read.read());
        	content += incoming;
        } while (incoming.length() > 0); // writer finished we shouldn't have any trouble here

        
        String[] c = content.split(":");
        assertEquals(100, c.length);
        for (String s : c) {
            assertEquals("abc", s);
        }
        reader.join();

        assertEquals(content, reader.result.toString());
    }

    @SuppressWarnings("resource")
	@Test
    public void fanOutRead() throws IOException, InterruptedException {
    	IPage write = new PageImpl(filename).createFile(PageMetadata.METADATA_SIZE + 1024);
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
        IPage reader = new PageImpl(filename).readOnly();
        do {
        	incoming = str(reader.read());
        	content += incoming;
        } while (incoming.length() > 0); // writer finished we shouldn't have any trouble here
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
        	IPage source = new PageImpl(filename).readOnly();
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
        private final IPage target;

        WriterThread(String payload, int interations, IPage target) {
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
