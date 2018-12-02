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


import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;

import communication.ObjectInputStream_DS;
import communication.ObjectOutputStream_DS;
import recipes_service.ServerData;

import recipes_service.data.*;
import recipes_service.tsae.data_structures.*;
import recipes_service.communication.*;
import java.util.*;

/**
 * @author Joan-Manuel Marques
 * December 2012
 *
 */
public class TSAESessionPartnerSide extends Thread{
	private Socket socket = null;
	private ServerData serverData = null;
	
	public TSAESessionPartnerSide(Socket socket, ServerData serverData) {
		super("TSAEPartnerSideThread");
		this.socket = socket;
		this.serverData = serverData;
	}

	/**
	 * 
	 */
	public void run() {
				
		int currentSessionNumber = -1;
		
		try {
			ObjectOutputStream_DS out = new ObjectOutputStream_DS(socket.getOutputStream());
			ObjectInputStream_DS in = new ObjectInputStream_DS(socket.getInputStream());

			TimestampVector localSummary;
			TimestampMatrix localAck;

			Message msg = (Message) in.readObject();
			currentSessionNumber = msg.getSessionNumber();
			
			if (msg.type() == MsgType.AE_REQUEST) {
                TimestampVector originatorSummary = ((MessageAErequest) msg).getSummary();
                TimestampMatrix originatorAck = ((MessageAErequest) msg).getAck(); // TODO
                
                // localSumary is a clone of local
                localSummary = serverData.getSummary().clone();
                String serverDataId=serverData.getId();
                serverData.getAck().update(serverDataId, localSummary);
                //localAck is a clone of local
                localAck = serverData.getAck().clone();
                
                // Get operations received 
                List<Operation> operations = null;
                synchronized (serverData.getCommunicationLock()) {
                    Log log = serverData.getLog();
                	operations = log.listNewer(originatorSummary);
                	localSummary = serverData.getSummary();
                	localAck = serverData.getAck();
                }
                
                // Write operations to Out
                for (Operation operation : operations) {
					msg = new MessageOperation(operation);
					msg.setSessionNumber(currentSessionNumber);
					out.writeObject(msg);
				}

                // send to originator: local's summary and ack
                msg = new MessageAErequest(localSummary, localAck);
				msg.setSessionNumber(currentSessionNumber);
                out.writeObject(msg);

                // receive operations
                msg = (Message) in.readObject();
                while (msg.type() == MsgType.OPERATION) {
                    Operation op = ((MessageOperation) msg).getOperation();
                    if(op.getType()== OperationType.ADD){
                        Recipe rcpe = ((AddOperation) op).getRecipe();
                        serverData.getRecipes().add(rcpe);
                    }
                    serverData.getLog().add(op);
                    msg = (Message) in.readObject();
                }

                // receive message to inform about the ending of the TSAE session
                if (msg.type() == MsgType.END_TSAE) {
                    serverData.getSummary().updateMax(originatorSummary);
                
                    serverData.getLog().purgeLog(serverData.getAck());
                    msg = new MessageEndTSAE();
                    msg.setSessionNumber(currentSessionNumber);
                    out.writeObject(msg);
                    
                    //TODO Operations Queue 
                    //for
                    // operations...
                    
                    
                }

            }
            socket.close();
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        } catch (IOException e) {


        }
	}
}