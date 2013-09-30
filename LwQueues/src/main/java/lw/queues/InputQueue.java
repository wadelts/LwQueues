package lw.queues;

import java.io.*;

// Import of Sun's JMS interface
import javax.jms.*;	// Using the one in C:\glassfish3\mq\lib\jms.jar

import java.util.logging.*;

import lw.utils.LwLogger;


/**
* This class will be used to provide 'get'access to a queue
* and will alllow the caller to read messages off this queue
*
* @author      Liam Wade
* @version     %I%
*
*/
public class InputQueue
{

    private static final Logger logger = Logger.getLogger("gemha");
	
	private Destination			ioQueue      = null;
	private Session				session      = null;
	private Connection			connection   = null;
	private ConnectionFactory	factory      = null;
	private MessageConsumer		messageConsumer  = null;

	private String replytoQueueURI = null;	// the ReplyToQ info in a received message in the form of a URI that
											// can be used in the creation methods to reconstruct an MQQueue object.
											// e.g "queue://ERROR_QUEUE_MANAGER/ERROR_QUEUE?targetClient=1"
  /**
    * Will create a new exception.
    */
	public InputQueue() {
	}

	 /**
        * This method creates a jms queueReader object
        *
        * @param queueName the name of the Queue - throws IllegalArgumentException if null
        * @param urlJMSserver the connection URL for the broker e.g. "mq://localhost:7676" for Glassfish - throws IllegalArgumentException if null
        * @return Void
        * @exception    GenericQueueException
        */
	public void open(String queueName, String urlJMSserver) throws GenericQueueException {
		checkNullArgument(queueName);
		checkNullArgument(urlJMSserver);

		try {
		    // URL of the JMS server. DEFAULT_BROKER_URL will just mean
		    // that JMS server is on localhost
//		    String urlJMSserver = ActiveMQConnection.DEFAULT_BROKER_URL;

		    //create a connection factory
			logger.info("Creating a QueueConnectionFactory on Broker " + urlJMSserver);
//			factory = new ActiveMQConnectionFactory(urlJMSserver);
			factory = new com.sun.messaging.ConnectionFactory();

			// Create a QueueConnection from the QueueConnectionFactory
			logger.finer("Creating a Connection");
			connection = factory.createConnection();

			// We now create a QueueSession from the connection. Here we
			// specify that it should be transacted, which means that the acknowledge param is ignored
			logger.finer("Creating a Session");
			boolean transacted = true;
			session = connection.createSession( transacted, Session.AUTO_ACKNOWLEDGE);

			// Use the session to create the queue object
			logger.info("Attaching to queue " + queueName);
			ioQueue = session.createQueue(queueName);

			// Create a QueueReceiver
			logger.finer( "Creating a QueueConsumer");
			messageConsumer = session.createConsumer(ioQueue);

			// IMPORTANT: Receive calls will be blocked if the connection is
			// not explicitly started, so make sure that we do so!
			logger.finer("Starting the Connection");
			connection.start();
		}
		catch(JMSException je) {
			logger.warning("Error: Exception caught " + je);

			Exception e = je.getLinkedException();
			if (e != null) {
				logger.warning("Error: linked exception: " + e);
			}

			// if we get an exception, we shoud close down any opened mq objects
			this.close(null);
			throw new GenericQueueException(je.getMessage() + e.getMessage());
		}
	}

	/**
        * This method takes the next message off the queue specified in the previously created messageConsumer object
        *
        * @param timeoutMillis the number of milliseconds to wait for a message - 0 implies block indefinitely
        *
        * @return String message contents
        * @exception GenericQueueException
        * @exception NoMessageFoundException
        */
	public String getNextMessage(long timeoutMillis) throws GenericQueueException, NoMessageFoundException {
		logger.finer( "Reading a new message" );

		String receivedMessage = null;

		try {
			Message inMessage = messageConsumer.receive(timeoutMillis);

			if(inMessage == null) {
				if (timeoutMillis == 0) { // then was supposed to block forever, but someone must have closed the consumer
					logger.finer( "The attempt to read a new message " +
					"failed, apparently because it wasn't there");
					throw new GenericQueueException("Stopped blocking on Queue. Someone closed Consumer.");
				}
				else {
					throw new NoMessageFoundException("Failed to get message. Had waited " + timeoutMillis + " milliseconds.");
				}
			}
			else {
				replytoQueueURI = null; // just to clear from previous message

				// LW - Cast to BytesMessage - casting to TextMessage fails, as data is actually byte-encoded
				if (inMessage instanceof BytesMessage) {
					BytesMessage inBytesMessage = ((BytesMessage) inMessage);

					// LW - OK to cast long to int here, as message won't have length greater than can be held in an int
					byte[] byteArray = new byte[(int)(inBytesMessage.getBodyLength())];
					inBytesMessage.readBytes(byteArray);
					receivedMessage = java.nio.charset.Charset.forName("UTF-8").decode(java.nio.ByteBuffer.wrap(byteArray)).toString();
				}
				else if (inMessage instanceof TextMessage) {
					TextMessage inTextMessage = ((TextMessage) inMessage);
					receivedMessage = inTextMessage.getText();
				}
				else {
					// Failed to cast to BytesMessage or TextMessage
					// BACKOUT TRANSACTION!
					logger.warning("Failed to cast message to BytesMessage or TextMessage");
					if (session != null && session.getTransacted()) {
						logger.warning("Rolling back transaction");
						session.rollback();
						logger.warning("Transaction rolled back");
					}

					throw new GenericQueueException("Couldn't handle message. Not Text or Bytes.");
				}

				// See if should save ReplyTo queue info...
				Destination replytoDest = null;
				if ( (replytoDest = inMessage.getJMSReplyTo()) != null) {
					if (replytoDest instanceof javax.jms.Queue) {
						javax.jms.Queue replytoQueue = (javax.jms.Queue)replytoDest;
						replytoQueueURI = replytoQueue.getQueueName();
						logger.finer("Replyto Queue is " + replytoQueueURI);
					}
				}

				return receivedMessage;
			}

		}
		catch(JMSException je) {
			logger.warning("Error: Exception caught " + je);

			Exception e = je.getLinkedException();
			if (e != null) {
				logger.warning("Error: linked exception: " + e);
			}

			throw new GenericQueueException(je.getMessage() + e.getMessage());
		}
  	}


	/**
        * Get the ReplyToQ name of the last-returned message
        *
        * @return String ReplyToQ name in the form of a URI that can be used in the creation methods to reconstruct an MQQueue object. Null none available.
        */
	public String getReplytoQueueURI() {
		return replytoQueueURI;
	}

	/**
        * This method rolls back receipt of a message in the previously created session object
        *
        * @exception GenericQueueException
        */
	public void sessionRollback()
							throws GenericQueueException {

		try {
			if (session != null && session.getTransacted()) {
				logger.warning("Rolling back transaction");
				session.rollback();
				logger.warning("Transaction rolled back");
			}
		}
		catch(JMSException je) {
			logger.warning("Error: Exception caught " + je);

			Exception e = je.getLinkedException();
			if (e != null) {
				logger.warning("Error: linked exception: " + e);
			}

			throw new GenericQueueException(je.getMessage() + e.getMessage());
		}
	}

	/**
        * This method commits receipt of a message in the previously created session object
        *
        * @exception GenericQueueException
        */
	public void sessionCommit()
							throws GenericQueueException {

		try {
			if (session != null && session.getTransacted()) {
				logger.info("committing transaction");
				session.commit();
				logger.info("Transaction committed");
			}
		}
		catch(JMSException je) {
			logger.warning("Error: Exception caught " + je);

			Exception e = je.getLinkedException();
			if (e != null) {
				logger.warning("Error: linked exception: " + e);
			}

			throw new GenericQueueException(je.getMessage() + e.getMessage());
		}
	}

	/**
        * This method close queue and associated resources previously setup in the open call
        *
        * @param out the Logger to use to report events etc
        */
	public void close(LwLogger out)
	{
		// LwLogger used when close() is called by
		// a VM shutdown hook, in which case the logger may be dead (it's shutdown hook may be
		// executed before ours), so a FileWriter object is used instead.

		// Closing session object (There is no need to close the producers and consumers of a closed session.)
		try{
			if (session != null) {
				session.close();
				session = null;

				if (out != null) {
					out.appendln("InputQueue.close(): session closed");
				}
				else {
					logger.warning("session closed");
				}
			}

			// Closing QueueConnection.bject
			if (connection != null) {
				connection.close();
				connection = null;

				if (out != null) {
					out.appendln("InputQueue.close(): connection closed");
				}
				else {
					logger.finer("connection closed");
				}
			}
		}
		catch(IOException e) {
			System.out.println("InputQueue.close(): While closing session or connection caught " + e.getMessage());
		}
		catch(Exception e) {
			if (out != null) {
				try {
					out.appendln("InputQueue.close(): While closing session or connection caught " + e.getMessage());
				}
				catch(IOException e2) {
					System.out.println("InputQueue.close(): While reporting Exception, caught " + e2.getMessage());
				}
			}
			else {
				logger.warning("While closing session or connection caught " + e.getMessage());
			}
		}
	}

	/**
	 * @param o the object to be checked for null.
	 * 
	 * @throws IllegalArgumentException if o is null
	 */
	private void checkNullArgument(Object o) {
		if ((o == null)) throw new IllegalArgumentException("[" + Thread.currentThread().getName() + "]: Null value received.");
	}
}