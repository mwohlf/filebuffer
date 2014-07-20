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

    private File file;

    @Before
    public void prepareFilename() throws IOException {
        file = File.createTempFile(
                getClass().getCanonicalName(),
                String.valueOf(Thread.currentThread().getId()));
        file.delete(); // created when the cache is initialized
    }

    @After
    public void cleanup() {
        if (file != null) {
            try {
            	file.delete();
            } catch (Exception ex) {
                // ignore
            }
        }
    }

	@Test
    public void smokeTest() throws IOException {
        IPage write = new PageImpl(file, PageMetadata.METADATA_SIZE + 70);
        write.createWriteBuffer();
        write.write(bb("blablablabla23"));
        assertEquals(70 - (PageImpl.INT_SIZE + PageImpl.INT_SIZE)
        		        - ("blablablabla23".getBytes().length + PageImpl.INT_SIZE), write.remainingForWrite());
        write.closeWriteBuffer();

        IPage read = new PageImpl(file, PageMetadata.METADATA_SIZE + 70);
        read.openReadBuffer();
        assertBufferEquals(bb("blablablabla23"), read.read());
        read.closeReadBuffer();
    }

	@Test
    public void doubleWrite() throws IOException {
    	IPage write = new PageImpl(file, PageMetadata.METADATA_SIZE + 70);
    	write.createWriteBuffer();
        write.write(bb("test1data"));
        assertEquals(70 - (PageImpl.INT_SIZE + PageImpl.INT_SIZE)
		        	    - ("test1data".getBytes().length + PageImpl.INT_SIZE), write.remainingForWrite());
        write.write(bb("2"));
        assertEquals(70 - (PageImpl.INT_SIZE + PageImpl.INT_SIZE)
		         - ("test1data".getBytes().length + PageImpl.INT_SIZE)
        		 - ("2".getBytes().length + PageImpl.INT_SIZE), write.remainingForWrite());
        write.closeWriteBuffer();

        IPage read = new PageImpl(file, PageMetadata.METADATA_SIZE + 70);
        read.openReadBuffer();
        assertBufferEquals(bb("test1data"), read.read());
        assertBufferEquals(bb("2"), read.read());
        read.closeReadBuffer();
    }
    
    @Test
    public void doubleRead() throws IOException {
    	IPage write = new PageImpl(file, PageMetadata.METADATA_SIZE + 70);
    	write.createWriteBuffer();
        write.write(bb("testdatabla"));
        write.closeWriteBuffer();

        IPage read = new PageImpl(file, PageMetadata.METADATA_SIZE + 70);
        read.openReadBuffer();
        assertEquals("testdatabla", str(read.read()));
        read.closeReadBuffer();

        // same data should be returned
        read = new PageImpl(file, PageMetadata.METADATA_SIZE + 70);
        read.openReadBuffer();
        assertEquals("testdatabla", str(read.read()));
        read.closeWriteBuffer();
    }

    @Test
    public void splitWrite() throws IOException {
    	IPage write = new PageImpl(file, PageMetadata.METADATA_SIZE + 70);
    	write.createWriteBuffer();
        write.write(bb("testda2ta"));
        write.closeWriteBuffer();

        // data should be appended
        write = new PageImpl(file, PageMetadata.METADATA_SIZE + 70);
        write.openWriteBuffer();
        write.write(bb("777"));
        write.closeWriteBuffer();

        IPage read = new PageImpl(file, PageMetadata.METADATA_SIZE + 70);
        read.openReadBuffer();
        assertBufferEquals(bb("testda2ta"), read.read());
        assertBufferEquals(bb("777"), read.read());
        read.closeReadBuffer();

        // param should be consumed
        write = new PageImpl(file, PageMetadata.METADATA_SIZE + 70);
        write.openWriteBuffer();
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
        write.closeWriteBuffer();
    }

    @Test
    public void doubleWriteExeption() {
    	IPage write2 = null;
        try {
        	IPage write1 = new PageImpl(file, PageMetadata.METADATA_SIZE + 70);
            write1.createWriteBuffer();
            write1.write(bb("testdata"));
            write1.closeWriteBuffer();

            write2 = new PageImpl(file, PageMetadata.METADATA_SIZE + 70);
            write2.createWriteBuffer();
            fail();
        } catch (CacheException ex) {
        	// ignore
        }
    }

    @Test
    public void sequentialReadWrite() throws IOException {
    	IPage write = new PageImpl(file, PageMetadata.METADATA_SIZE + 250);
    	write.createWriteBuffer();
        write.write(bb("hüzelbrützel"));
        write.closeWriteBuffer();

        IPage read = new PageImpl(file, PageMetadata.METADATA_SIZE + 250);
        read.openReadBuffer();
        assertEquals("hüzelbrützel", str(read.read()));
        read.closeReadBuffer();

        write = new PageImpl(file, PageMetadata.METADATA_SIZE + 250);
        write.openWriteBuffer();
        write.write(bb("maultaschen"));
        write.closeWriteBuffer();

        read = new PageImpl(file, PageMetadata.METADATA_SIZE + 250);
        read.openReadBuffer();
        assertEquals("hüzelbrützel", str(read.read()));
        assertEquals("maultaschen", str(read.read()));
        read.closeReadBuffer();

        read = new PageImpl(file, PageMetadata.METADATA_SIZE + 250);
        read.openReadBuffer();
        assertEquals("hüzelbrützel", str(read.read()));
        assertEquals("maultaschen", str(read.read()));
        read.closeReadBuffer();

        write = new PageImpl(file, PageMetadata.METADATA_SIZE + 250);
        write.openWriteBuffer();
        write.write(bb("bretzelbrötchen"));
        write.closeWriteBuffer();

        read = new PageImpl(file, PageMetadata.METADATA_SIZE + 250);
        read.openReadBuffer();
        assertEquals("hüzelbrützel", str(read.read()));
        assertEquals("maultaschen", str(read.read()));
        assertEquals("bretzelbrötchen", str(read.read()));
        read.closeReadBuffer();
    }


    @SuppressWarnings("resource")
    @Test
    public void writeIntoFullPage() throws IOException {
        // stuff in the file:
        // 8 byte index, 4 byte limit, (not part of the payload)
        // 4 byte chunksize, 4 byte EOF
    	IPage write = new PageImpl(file, PageMetadata.METADATA_SIZE + 19); 
    	write.createWriteBuffer();
        ByteBuffer param = bb("12345678901234567890");  // 20 
        write.write(param);
        assertEquals(20, param.remaining());

        // 35 minus HEADER_SIZE (16) --> we have 19 to write
        String string = "1234567890123456789";
        param = bb(string);
        write.write(param);
        assertEquals(19, param.remaining());
        
        // we need 4 for the limit pointer  --> 15
        string = "1234567890123456789".substring(PageImpl.INT_SIZE);
        param = bb(string);
        write.write(param);
        assertEquals(string.length(), param.remaining());

        // we need another 4 for the EOF pointer --> 11
        string = "1234567890123456789".substring(PageImpl.INT_SIZE  + PageImpl.INT_SIZE);
        param = bb(string);
        write.write(param);
        assertEquals(0, param.remaining());
       
        // no more room to write
        String s = ".";
        param = bb(s);
        write.write(param);
        assertEquals(s.toCharArray().length, param.remaining());

        // check if we can read the stuff again
        IPage read = new PageImpl(file, PageMetadata.METADATA_SIZE + 19); 
        read.openReadBuffer();
        assertBufferEquals(bb(string), read.read());
              
        // stil no more room to write?
        String t = ".";
        param = bb(t);
        write.write(param);
        assertEquals(t.toCharArray().length, param.remaining());
    }

    
    @Test
    public void checkUnderflow() throws IOException {
        ByteBuffer param;

        IPage write = new PageImpl(file, PageMetadata.METADATA_SIZE + 20);
        write.createWriteBuffer();
        
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
    	IPage write = new PageImpl(file, PageMetadata.METADATA_SIZE + 70);
    	write.createWriteBuffer();
        write.closeWriteBuffer();

        IPage read = new PageImpl(file, PageMetadata.METADATA_SIZE + 70);
        read.openReadBuffer();
        assertEquals("", str(read.read()));
        assertEquals("", str(read.read()));
    }

    @Test
    public void doubleCreate() throws IOException, InterruptedException {
        try {
        	IPage page = new PageImpl(file, 70);
        	page.createWriteBuffer();
        	page.createWriteBuffer();
            fail("two write instances allowed");
        } catch (CacheException ex) {
            // expectd
        }
    }

    @Test
    public void concurrentReadWrite() throws IOException, InterruptedException {
    	
    	IPage page = new PageImpl(file, 1024);
    	
        WriterThread writer = new WriterThread("abc:", 100, page);
        page.createWriteBuffer();
        ReaderThread reader = new ReaderThread(page);

        reader.start();
        writer.start();
        writer.join();
        reader.writerrunning = false;

        String content = "";
        String incoming = "";
        
    	IPage read = new PageImpl(file, 1024);
        read.openReadBuffer();
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

	@Test
    public void fanOutRead() throws IOException, InterruptedException {
		
    	IPage page = new PageImpl(file, 1024);

        int iterations = 3;

        WriterThread writer = new WriterThread("bla:", iterations, page);
        page.createWriteBuffer();

        ReaderThread reader1 = new ReaderThread(page);
        ReaderThread reader2 = new ReaderThread(page);
        ReaderThread reader3 = new ReaderThread(page);
        ReaderThread reader4 = new ReaderThread(page);

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
        IPage reader = new PageImpl(file, 1024);
        reader.openReadBuffer();

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
        private volatile IPage source;
        private volatile StringBuilder result = new StringBuilder();
        private volatile boolean writerrunning= true;

        ReaderThread(IPage source) {
        	this.source = source;
        }

        @Override
        public void run() {
        	source.openReadBuffer();
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
