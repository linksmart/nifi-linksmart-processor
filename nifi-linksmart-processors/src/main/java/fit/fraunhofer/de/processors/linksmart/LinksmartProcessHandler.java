package fit.fraunhofer.de.processors.linksmart;

import com.zaxxer.nuprocess.NuAbstractProcessHandler;
import com.zaxxer.nuprocess.NuProcess;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;


class LinksmartProcessHandler extends NuAbstractProcessHandler {

    private NuProcess nuProcess;
    private BlockingQueue<String> stdoutQueue;
    private BlockingQueue<String> stderrQueue;
    private ByteArrayOutputStream currBytes = new ByteArrayOutputStream(4096);

    public LinksmartProcessHandler(BlockingQueue<String> stdoutQueue, BlockingQueue<String> stderrQueue) {
        this.stdoutQueue = stdoutQueue;
        this.stderrQueue = stderrQueue;
    }

    @Override
    public void onStart(NuProcess nuProcess) {
        this.nuProcess = nuProcess;
    }

    @Override
    public void onStdout(ByteBuffer buffer, boolean closed) {
        System.out.println("onStdout method called with closed: " + closed);

        if (!closed) {
            int total = buffer.remaining();

            currBytes.reset();
            // Mark the beginning of buffer to come back, in case no delimiter is found
            buffer.mark();
            for (int i = 0; i < total; i++) {

                byte currByte = buffer.get();

                // TODO: consider whether to write newline char to message
                currBytes.write(currByte);
                // Separate bytes into messages
                if (currByte == '\n') {
                    // TODO: handle stdoutQueue full situation
                    String msg = new String(currBytes.toByteArray(), StandardCharsets.UTF_8);
                    stdoutQueue.offer(msg.replace("\r", "").replace("\n", ""));

                    System.out.println("New stdout message put into queue: " + msg);

                    // Mark the break point, so that next read will always start here
                    buffer.mark();
                    currBytes.reset();
                }
            }

            // Reset to the last break point
            buffer.reset();

        }
    }

    @Override
    public void onStderr(ByteBuffer buffer, boolean closed) {
        System.out.println("onStderr method called with closed: " + closed);

        if (!closed) {
            byte[] currBytes = new byte[buffer.remaining()];
            buffer.get(currBytes);
            String msg = new String(currBytes);
            stderrQueue.offer(msg);

            System.out.println("New stderr message put into queue: " + msg);

            // For this example, we're done, so closing STDIN will cause the "cat" process to exit
            nuProcess.closeStdin(true);
        }

    }
}
