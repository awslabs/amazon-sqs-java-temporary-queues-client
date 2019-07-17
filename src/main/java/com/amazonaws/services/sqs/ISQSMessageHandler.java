package com.amazonaws.services.sqs;

import com.amazonaws.services.sqs.model.Message;

/**
 * The Interface ISQSMessageHandler.
 */
public interface ISQSMessageHandler {
	
	/**
	 * Handle message.
	 *
	 * @param msg the msg
	 */
	public void handleMessage(Message msg);
}
