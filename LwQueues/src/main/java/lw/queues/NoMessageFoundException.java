package lw.queues;

/**
  * Encapsulates exceptions resulting from errors returned from queue activity.
  * @author Liam Wade
  * @version 1.0 25/06/2003
  */
public class NoMessageFoundException extends Exception
{
  /**
    * Will create a new exception.
    */
	public NoMessageFoundException() {
	}

  /**
    * Will create a new exception with the given reason.
	* @param reason the text explaining the error
    */
	public NoMessageFoundException(String reason) {
		super(reason);
	}

}