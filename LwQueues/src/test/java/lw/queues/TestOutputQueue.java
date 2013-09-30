package lw.queues;

import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author wadel
 *
 */
public class TestOutputQueue {
	static String testQueueName = "LWqueue004";
	static String urlJMSserver = "mq://localhost:7676";
	
	String testMessage = "Am I the same message?";

	@BeforeClass
	public static void purgeMessages() {
		InputQueue inQueue = new InputQueue();

		try {
			inQueue.open(testQueueName, urlJMSserver);
		}
		catch (GenericQueueException e) {
			fail("GenericQueueException encountered opening Input queue: " + e);
		}
		
		try {
			String receivedMessage = null;
			do {
				int waitInterval = 1000; // how many milliseconds to wait for a message before returning nothing (0 = block indefinitely)
				receivedMessage = inQueue.getNextMessage(waitInterval);
				inQueue.sessionCommit();
			} while (receivedMessage != null);
		} catch (GenericQueueException e) {
			fail("GenericQueueException encountered getting testMessage: " + e);
		} catch (NoMessageFoundException e) {
			// Ignore, we just want to empty queue, to ensure have clean slate to start with
		}
		
		inQueue.close(null);
	}
	
	@Test
	public void testSendMessage() {
		// I placed both tests in same Test as I want them run in order. (JUnit doesn't guarantee run-order.)
		// Place the message on the queue
		put(testMessage);

		// Remove the message again...
		String receivedMessage = getMessage();
		assertEquals("Message different after retrieval", testMessage, receivedMessage);
	}
	
	/**
	 * @param message the message to be sent to the queue
	 */
	public void put(String message) {
		OutputQueue outQueue = new OutputQueue();

		try {
			outQueue.open(testQueueName, urlJMSserver);
		}
		catch (GenericQueueException e) {
			fail("GenericQueueException encountered opening Output queue: " + e);
		}
		
		try {
			outQueue.sendMessage(message);
		} catch (GenericQueueException e) {
			fail("GenericQueueException encountered sending testMessage: " + e);
		}
		
		outQueue.close();
	}

	/**
	 * @return the message received
	 */
	private String getMessage() {
		InputQueue inQueue = new InputQueue();
		String receivedMessage = null;

		try {
			inQueue.open(testQueueName, urlJMSserver);
		}
		catch (GenericQueueException e) {
			fail("GenericQueueException encountered opening queue: " + e);
		}
		
		try {
			int waitInterval = 1000; // how many milliseconds to wait for a message before returning nothing (0 = block indefinitely)
			receivedMessage = inQueue.getNextMessage(waitInterval);
			inQueue.sessionCommit();
		} catch (GenericQueueException e) {
			fail("GenericQueueException encountered getting testMessage: " + e);
		} catch (NoMessageFoundException e) {
			fail("NoMessageFoundException encountered getting testMessage: " + e);
		}
		
	
		inQueue.close(null);

		return receivedMessage;
	}

}
