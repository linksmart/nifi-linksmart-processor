package fit.fraunhofer.de.processors.linksmart;

import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.assertEquals;

public class LinksmartProcessHandlerTest {

    private BlockingQueue<String> stdoutQueue;
    private BlockingQueue<String> stderrQueue;
    private LinksmartProcessHandler handler;

    @Before
    public void init () {
        stdoutQueue = new LinkedBlockingQueue<>();
        stderrQueue = new LinkedBlockingQueue<>();
        handler = new LinksmartProcessHandler(stdoutQueue, stderrQueue);
    }

    @Test
    public void testOnStdoutGetSingleMessageAtOnce () {

        ByteBuffer buffer = ByteBuffer.wrap("Message 1\n".getBytes());
        handler.onStdout(buffer, false);

        assertEquals("Only 1 message should be in the queue", 1, stdoutQueue.size());
        assertEquals("Message content not the same", "Message 1", stdoutQueue.remove());
    }

    @Test
    public void testOnStdoutGetMultipleMessageAtOnce () {

        ByteBuffer buffer = ByteBuffer.wrap("Message 1\nMessage 2\nMessage 3\n".getBytes());
        handler.onStdout(buffer, false);

        assertEquals("Number of message in queue not correct", 3, stdoutQueue.size());

    }

    @Test
    public void testOnStdoutGetPartialMessage () {

        ByteBuffer buffer = ByteBuffer.allocate(100);
        buffer.put("Message part 1, ".getBytes());
        buffer.flip();
        handler.onStdout(buffer, false);

        buffer.compact();
        buffer.put("Message part 2\n".getBytes());
        buffer.flip();

        handler.onStdout(buffer, false);

        assertEquals("Number of message in queue not correct", 1, stdoutQueue.size());
        assertEquals("Merged message content not the same", "Message part 1, Message part 2", stdoutQueue.remove());

    }

}
