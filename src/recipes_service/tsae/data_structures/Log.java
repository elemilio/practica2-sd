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
	
	/*
	public synchronized boolean add(Operation op){
		List<Operation> principalLog = log.get(op.getTimestamp().getHostid());
		if (principalLog.size() > 0) {
			Operation lastOp = principalLog.get(principalLog.size() - 1);
			if (lastOp.getTimestamp().compare(op.getTimestamp()) >= 0) {
				return false;
			}
		}
		principalLog.add(op);
		log.put(op.getTimestamp().getHostid(), principalLog);
		return true;
	}
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


	
	/**
	 * 
	 * @param summary
	 * @return
	 */
	public synchronized List<Operation> listNewer(TimestampVector sum) {

		List<Operation> list = new Vector<Operation>();
		List<String> participants = new Vector<String>(this.log.keySet());

		for (Iterator<String> it = participants.iterator(); it.hasNext(); ){
			String node = it.next();
			List<Operation> operations = new Vector<Operation>(this.log.get(node));
			Timestamp timestampToCompare = sum.getLast(node);

			for (Iterator<Operation> operationIterator = operations.iterator(); operationIterator.hasNext(); ) {
				Operation operation = operationIterator.next();
				
				if (operation.getTimestamp().compare(timestampToCompare) > 0) {
					list.add(operation);
				}
			}
		}
		return list;
	}
	


	/**
	 * Removes all operations ack by all hosts
	 * @param ack
	 */
	public synchronized void purgeLog(TimestampMatrix ack){
		
		List<String> keys = new Vector<String>(this.log.keySet());
		TimestampVector min = ack.minTimestampVector();
		
		for (Iterator<String> iterator = keys.iterator(); iterator.hasNext(); ){
			String node = iterator.next();
			
			for (Iterator<Operation> operation = log.get(node).iterator(); operation.hasNext();) {
				if (min.getLast(node) != null && operation.next().getTimestamp().compare(min.getLast(node)) <= 0) {
					operation.remove();
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