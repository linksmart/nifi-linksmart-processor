package fit.fraunhofer.de.processors.linksmart;

import com.zaxxer.nuprocess.NuProcess;
import com.zaxxer.nuprocess.NuProcessBuilder;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


import static org.junit.Assert.assertEquals;

public class NuProcessTest {

    private BlockingQueue<String> stdoutQueue;
    private BlockingQueue<String> stderrQueue;

    @Before
    public void init () {
        stdoutQueue = new LinkedBlockingQueue<>();
        stderrQueue = new LinkedBlockingQueue<>();
    }

    @Test
    public void testGetSingleMessage () {
        String[] cmdArray = getCommandArray("print_one_msg.py");

        NuProcessBuilder pb = new NuProcessBuilder(cmdArray);
        LinksmartProcessHandler handler = new LinksmartProcessHandler(stdoutQueue, stderrQueue);
        pb.setProcessListener(handler);
        NuProcess process = pb.start();

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        process.destroy(false);

        assertEquals("Only 1 message should be in the queue", 1, stdoutQueue.size());
        assertEquals("Message content not the same", "Python printed 1 message", stdoutQueue.remove());

    }

    @Test
    public void testGetMultipleMessage () {
        String[] cmdArray = getCommandArray("print_multiple_msg.py");

        NuProcessBuilder pb = new NuProcessBuilder(cmdArray);
        LinksmartProcessHandler handler = new LinksmartProcessHandler(stdoutQueue, stderrQueue);
        pb.setProcessListener(handler);
        NuProcess process = pb.start();

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        process.destroy(false);

        assertEquals("Only 1 message should be in the queue", 3, stdoutQueue.size());
    }


    @Test
    public void testGetException () throws IOException {
        String[] cmdArray = getCommandArray("get_exception.py");

        NuProcessBuilder pb = new NuProcessBuilder(cmdArray);
        LinksmartProcessHandler handler = new LinksmartProcessHandler(stdoutQueue, stderrQueue);
        pb.setProcessListener(handler);
        NuProcess process = pb.start();

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        process.destroy(false);

        assertEquals("Only 1 message should be in the queue", 1, stderrQueue.size());

    }

    /*@Test
    public void testInterruptSubprocess () throws Exception {
        String[] cmdArray = getCommandArray("catch_sigterm.py");

        NuProcessBuilder pb = new NuProcessBuilder(cmdArray);
        LinksmartProcessHandler handler = new LinksmartProcessHandler(stdoutQueue, stderrQueue);
        pb.setProcessListener(handler);
        NuProcess process = pb.start();

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("Soft-killing subprocess...");
        process.destroy(false);
        process.waitFor(5, TimeUnit.SECONDS);

    }*/

    private String[] getCommandArray(String fileName) {

        String relPath = "src\\test\\resources\\" + fileName;
        String absPath = new File(relPath).getAbsolutePath();
        String cmd = "python " + absPath;
        return cmd.trim().split("\\s+");
    }
}
