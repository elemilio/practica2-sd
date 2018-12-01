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

package recipes_service.tsae.data_structures;

import java.io.Serializable;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import recipes_service.data.Operation;

/**
 * @author Joan-Manuel Marques, Daniel Lázaro Iglesias
 * December 2012
 *
 */
public class Log implements Serializable{

	private static final long serialVersionUID = -4864990265268259700L;
	/**
	 * This class implements a log, that stores the operations
	 * received  by a client.
	 * They are stored in a ConcurrentHashMap (a hash table),
	 * that stores a list of operations for each member of 
	 * the group.
	 */
	private ConcurrentHashMap<String, List<Operation>> log= new ConcurrentHashMap<String, List<Operation>>();
	private Semaphore mutex = new Semaphore(1);

	public Log(List<String> participants){
		// create an empty log
		for (Iterator<String> it = participants.iterator(); it.hasNext(); ){
			log.put(it.next(), new Vector<Operation>());
		}
	}

	/**
	 * @param op
	 * @return true insertion ok, false ko.
	 */
	public synchronized boolean add(Operation op){
		try {
			mutex.acquire();

			Timestamp opTimestamp = op.getTimestamp();
			String hostId = opTimestamp.getHostid();


			List<Operation> operations = log.get(hostId);
			
			if (operations.size() > 0) {
				Operation lastOperation = operations.get(operations.size() - 1);

				if (opTimestamp.compare(lastOperation.getTimestamp()) > 0) {
					log.get(hostId).add(op);
				} else {
					return false;
				}
			} else {
				log.get(hostId).add(op);
			}

		} catch (InterruptedException e) {
			return false;
		} finally {
			mutex.release();
		}

		return true;
	}

	
	private Timestamp getLastTimestamp(String hostID) {

      List<Operation> operations = this.log.get(hostID);
      Timestamp ret = null;
      
      if (operations != null && !operations.isEmpty()) {
    	  int lastOperationsList=operations.size() - 1;
    	  ret = operations.get(lastOperationsList).getTimestamp();
      }
		
      return ret;
	}
	

	/**
	 * 
	 * @param summary
	 * @return
	 */
	public synchronized List<Operation> listNewer (TimestampVector summary){
		
		List<Operation> operations = new ArrayList();

		for (String hostId : this.log.keySet()) {
			Timestamp lastTimestamp = summary.getLast(hostId);
			
			List<Operation> hostOperations = this.log.get(hostId);
			for (Operation hostOperation : hostOperations) {
				
				if (hostOperation.getTimestamp().compare(lastTimestamp) < 1) {
					operations.add(hostOperation);
				}
			}
		}

		return operations;

	}
	
	/**
	 * Removes all operations ack by all hosts
	 * @param ack
	 */
	public synchronized void purgeLog(TimestampMatrix ack){
		
		TimestampVector minTimestampVector = ack.minTimestampVector();
		
		for (Map.Entry < String, List<Operation>> entry : log.entrySet()){
			
			String entryKey = entry.getKey();
			List <Operation> operations = entry.getValue();
			Timestamp lastTimestamp = minTimestampVector.getLast(entryKey);
			
			for (int i = operations.size()-1; i >= 0; i--){
				
				Operation operation = operations.get(i);
				if (operation.getTimestamp().compare(lastTimestamp) < 0){
					operations.remove(i);
				}
			}
			
		}
		
		
	}

	/**
	 * equals
	 * added synchronized
	 */
	@Override
	public synchronized boolean equals(Object obj) {
		
		// it's exact
		if (this == obj)
			return true;
		
		// it's null
		if (obj == null)
			return false;
		
		// its'n same object type
		if (getClass() != obj.getClass())
			return false;
		
		// it's a Log object
		Log other = (Log) obj;
		
		// Log it's null
		if (log == null) {
			return other.log == null;
		} else {
			
			if (log.size() != other.log.size()){
				return false;
			}
			
			// Compare 2 Logs
			boolean equal = true;
			for (Iterator<String> iterator = log.keySet().iterator(); iterator.hasNext() && equal; ){
				
				String hostName = iterator.next();
				equal = log.get(hostName).equals(other.log.get(hostName));
				//equals
			}
			return equal;
		}

	}

	/**
	 * toString
	 */
	@Override
	public synchronized String toString() {
		String name="";
		for(Enumeration<List<Operation>> en=log.elements();
		en.hasMoreElements(); ){
		List<Operation> sublog=en.nextElement();
		for(ListIterator<Operation> en2=sublog.listIterator(); en2.hasNext();){
			name+=en2.next().toString()+"\n";
		}
	}
		
		return name;
	}

}