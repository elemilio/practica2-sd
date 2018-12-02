/*
* Copyright (c) Joan-Manuel Marques 2013. All rights reserved.
* DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
*
* This file is part of the practical assignment of Distributed Systems course.
*
* This code is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This code is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this code.  If not, see <http://www.gnu.org/licenses/>.
*/

package recipes_service.tsae.sessions;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TimerTask;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import recipes_service.ServerData;
import recipes_service.activity_simulation.SimulationData;
import recipes_service.communication.Host;
import recipes_service.communication.Message;
import recipes_service.communication.MessageAErequest;
import recipes_service.communication.MessageEndTSAE;
import recipes_service.communication.MessageOperation;
import recipes_service.communication.MsgType;
import recipes_service.data.AddOperation;
import recipes_service.data.Operation;
import recipes_service.data.OperationType;
import recipes_service.data.Recipe;
import recipes_service.tsae.data_structures.Log;
import recipes_service.tsae.data_structures.TimestampMatrix;
import recipes_service.tsae.data_structures.TimestampVector;
import communication.ObjectInputStream_DS;
import communication.ObjectOutputStream_DS;

//LSim logging system imports sgeag@2017
import lsim.worker.LSimWorker;
import edu.uoc.dpcs.lsim.LSimFactory;
import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;

/**
 * @author Joan-Manuel Marques
 * December 2012
 *
 */
public class TSAESessionOriginatorSide extends TimerTask{
	
	private static AtomicInteger session_number = new AtomicInteger(0);
	
	private ServerData serverData;
	public TSAESessionOriginatorSide(ServerData serverData){
		super();
		this.serverData=serverData;		
	}
	
	/**
	 * Implementation of the TimeStamped Anti-Entropy protocol
	 */
	public void run(){
		sessionWithN(serverData.getNumberSessions());
	}

	/**
	 * This method performs num TSAE sessions
	 * with num random servers
	 * @param num
	 */
	public void sessionWithN(int num){
		if(!SimulationData.getInstance().isConnected())
			return;
		List<Host> partnersTSAEsession= serverData.getRandomPartners(num);
		Host n;
		for(int i=0; i<partnersTSAEsession.size(); i++){
			n=partnersTSAEsession.get(i);
			sessionTSAE(n);
		}
	}
	
	/**
	 * This method perform a TSAE session
	 * with the partner server n
	 * @param n
	 */
	
	
	
	private void sessionTSAE(Host n){
		
		int currentSessionNumber = session_number.incrementAndGet();
		if (n == null) return;

		
		try {
			Socket socket = new Socket(n.getAddress(), n.getPort());
			ObjectInputStream_DS in = new ObjectInputStream_DS(socket.getInputStream());
			ObjectOutputStream_DS out = new ObjectOutputStream_DS(socket.getOutputStream());

			TimestampVector localSummary = null;
			TimestampMatrix localAck = null;
			
			List<Operation> operationsReceived = new ArrayList<Operation>();
			
			// Get local summary and ack	
			synchronized (serverData.getCommunicationLock()) {
				localSummary = serverData.getSummary().clone();
				localAck = serverData.getAck().clone();
			}
			
			String serverDataId = serverData.getId();
			serverData.getAck().update(serverDataId, localSummary);
			//localAck.update(serverDataId, localSummary);
			
			// Send local summary and ack
			Message	message = new MessageAErequest(localSummary, localAck);
			message.setSessionNumber(currentSessionNumber);
            out.writeObject(message);

            // Receive operations from partner
            message = (Message) in.readObject();

			while (message.type() == MsgType.OPERATION){

				Operation operation = ((MessageOperation) message).getOperation();
				operationsReceived.add(operation);
			
				
				if(operation.getType()== OperationType.ADD){
                    Recipe recipe = ((AddOperation) operation).getRecipe();
                    serverData.getRecipes().add(recipe);
                }
                serverData.getLog().add(operation);
                
                message = (Message) in.readObject();

			}

            // receive partner's summary and ack
			if (message.type() == MsgType.AE_REQUEST){

				// send operations

				TimestampVector partnerSummary = ((MessageAErequest) message).getSummary();
				TimestampMatrix partnerAck = ((MessageAErequest) message).getAck();

				List<Operation> operations = serverData.getLog().listNewer(partnerSummary);

				//if (operations != null) {

					for (Operation operation : operations) {
						message = new MessageOperation(operation);
						message.setSessionNumber(currentSessionNumber);
						out.writeObject(message);
					}
				//}


				// send and "end of TSAE session" message
				message = new MessageEndTSAE();  
				message.setSessionNumber(currentSessionNumber);
	            out.writeObject(message);					


				// receive message to inform about the ending of the TSAE session
	            message = (Message) in.readObject();

	            
				if (message.type() == MsgType.END_TSAE){
					serverData.getSummary().updateMax(partnerSummary);
					serverData.getAck().updateMax(partnerAck);
					serverData.getLog().purgeLog(serverData.getAck());

				}

			}			
			socket.close();
			
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block

			e.printStackTrace();
            System.exit(1);
		}catch (IOException e) {
	    }

	}
}