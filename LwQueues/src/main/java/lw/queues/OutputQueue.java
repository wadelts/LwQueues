package lw.queues;


// Import of Sun's JMS interface
import javax.jms.*;	// Using the one in C:\glassfish3\mq\lib\jms.jar

import java.util.logging.*;

//import org.apache.activemq.ActiveMQConnection;
//import org.apache.activemq.ActiveMQConnectionFactory;



/**
* This class will be used to provide 'put'access to a queue
* and will allow the caller to place message on this queue
*
* @author      Liam Wade
*
*/

public class OutputQueue
{

    private static final Logger logger = Logger.getLogger("gemha");

	private Destination			destination      = null;
    private Session				session      = null;
	private Connection			connection   = null;
    private ConnectionFactory	factory = null;
    private MessageProducer		producer = null;


  /**
    * Will create a new OutputQueue.
    *
    * @param logger the Logger to use to report errors, events etc
    */
	public OutputQueue() {
	}

	/**
        * This method creates a jms MessageProducer object
        *
        * @param queueName String holding the name of the Queue Manager - throws IllegalArgumentException if null
        * @param urlJMSserver String holding the name of the Queue - throws IllegalArgumentException if null
        * @return Void
        * @exception    GenericQueueException
        */
	public void open(String queueName, String urlJMSserver) throws GenericQueueException
	{
		checkNullArgument(queueName);
		checkNullArgument(urlJMSserver);

		logger.entering("OutputQueue", "open");


		try
		{
			//////////////////////////////////////////////////////////////////////////
			//create a connection factory
			//////////////////////////////////////////////////////////////////////////
		    // URL of the JMS server. DEFAULT_BROKER_URL will just mean
		    // that JMS server is on localhost
//		    String urlJMSserver = ActiveMQConnection.DEFAULT_BROKER_URL;

		    logger.info("Creating a ConnectionFactory on Broker " + urlJMSserver);
//			factory = new ActiveMQConnectionFactory(urlJMSserver);
//			factory.setProperty(ConnectionConfiguration.imqAddressList, "localhost:7676,broker2:5000,broker3:9999");
			factory = new com.sun.messaging.ConnectionFactory();
			

			//////////////////////////////////////////////////////////////////////////
			// Create a QueueConnection from the QueueConnectionFactory
			//////////////////////////////////////////////////////////////////////////
			logger.finer("Creating a Connection");
			connection = factory.createConnection();

			//////////////////////////////////////////////////////////////////////////
			// IMPORTANT: Receive calls will be blocked if the connection is
			// not explicitly started, so make sure that we do so!
			//////////////////////////////////////////////////////////////////////////
			logger.finer("Starting the Connection");
			connection.start();

			//////////////////////////////////////////////////////////////////////////
			// We now create a QueueSession from the connection. Here we
			// specify that it shouldn't be transacted, and that it should
			// automatically acknowledge received messages
			//////////////////////////////////////////////////////////////////////////
			logger.finer("Creating a Session");
			boolean transacted = false;
			session = connection.createSession( transacted, Session.AUTO_ACKNOWLEDGE);

			logger.info( "Opening Queue " + queueName);
			// Doesn't actually create a Queue, just a "queue identity" - createTemporaryQueue
			// is used to actually create queues on the fly
			destination = session.createQueue(queueName);

			//////////////////////////////////////////////////////////////////////////
			// Create a QueueSender
			//////////////////////////////////////////////////////////////////////////
			logger.finer( "Creating a QueueSender");
			producer = session.createProducer(destination);
		}
		catch(JMSException je) {
			logger.warning("Error: Exception caught " + je);

			Exception e = je.getLinkedException();
			if (e != null) {
				logger.warning("Error: linked exception: " + e);
			}

			// if we get an exception, we shoud close down any opened mq objects
			this.close();
			throw new GenericQueueException(je.getMessage() + e.getMessage());
	    }

		logger.exiting("OutputQueue", "open");
	}


	/**
        * This method puts a message on the queue specified in the previously-created queueSender object
        *
        * @param sMessage holds the actual message
        * @param sMsgType a message type on which to filter selection of messages
        * @exception GenericQueueException
        */
	public void sendMessage(String sMessage) throws  GenericQueueException
	{
		try {
			TextMessage outMessage = session.createTextMessage();
			logger.finer("Adding Text and message type");

			outMessage.setText(sMessage);

			logger.fine( "Sending message:" + sMessage );
			producer.send(outMessage);
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
		* This method puts a message on the queue specified in the previously-created queueSender object
		*
        * @param sMessage holds the actual message
        * @param sMsgType a message type on which to filter selection of messages
		* @param replyToQ the queue to which messages should be returned
		* @exception    GenericQueueException
		*/
	public void sendReplyMessage(String sMessage, String replyToQ)
															throws  GenericQueueException {

		logger.finer( "Creating a TextMessage using " + sMessage );
		try
		{
			TextMessage outMessage = session.createTextMessage();
			// Doesn't actually create a Queue, just a "queue identity" - createTemporaryQueue
			// is used to actually create queues on the fly
			Destination replyToQueue = session.createQueue(replyToQ);

			outMessage.setJMSReplyTo(replyToQueue);
			outMessage.setText(sMessage);
			producer.send(outMessage);
			logger.finer("Message has been successfully put on queue");
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
		* This method closes the queue and associated resources previously setup in the open call
		*
		*/
	public void close()
	{
		// Closing Producer
		if (producer != null) {
			logger.info("closing producer queue...");

			try {
				producer.close();
				producer=null;
			}
			catch (Exception e) {
				logger.warning("Error: While closing producer queue, caught " + e.getMessage());
			}
		}

		// Closing session object
		if (session != null) {
			logger.info("closing session");
			try {
				session.close();
				session=null;
			}
			catch (Exception e) {
				logger.warning("Error: While closing session caught " + e.getMessage());
			}
		}

		// Closing QueueConnection.bject
		if (connection != null) {
			logger.info("closing connection");
			try {
				connection.close();
				connection=null;
			}
			catch (Exception e) {
					logger.warning("Error: While closing connection caught " + e.getMessage());
			}
		}

		logger.info("All closed successfully");
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
