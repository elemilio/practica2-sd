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
import java.util.concurrent.ConcurrentHashMap;
/*TODO**/
import java.util.*;

/**
 * @author Joan-Manuel Marques, Daniel Lázaro Iglesias
 * December 2012
 *
 */
public class TimestampMatrix implements Serializable{

	private static final long serialVersionUID = 3331148113387926667L;
	ConcurrentHashMap<String, TimestampVector> timestampMatrix = new ConcurrentHashMap<String, TimestampVector>();
	
	public TimestampMatrix(List<String> participants){
		// create and empty TimestampMatrix
		for (Iterator<String> it = participants.iterator(); it.hasNext(); ){
			timestampMatrix.put(it.next(), new TimestampVector(participants));
		}
	}
	//OK
	private TimestampMatrix(ConcurrentHashMap<String, TimestampVector> timestampMatrix) {
		
		this.timestampMatrix = new ConcurrentHashMap<String, TimestampVector>(timestampMatrix.size());
		
		for (String key : timestampMatrix.keySet()) {
			this.timestampMatrix.put(key, timestampMatrix.get(key).clone());
		}
	}
	
	/**
	 * @param node
	 * @return the timestamp vector of node in this timestamp matrix
	 */
	private synchronized TimestampVector getTimestampVector(String node){
		return timestampMatrix.get(node);
	}
	
	/**
	 * Merges two timestamp matrix taking the elementwise maximum
	 * @param tsMatrix
	 */
	public synchronized void updateMax(TimestampMatrix tsMatrix){
		Enumeration<String> timestampMatrixKeys = timestampMatrix.keys();
		
		while (timestampMatrixKeys.hasMoreElements()) {
			String key = timestampMatrixKeys.nextElement();
			synchronized (key) {
				TimestampVector thisTimestampVector = getTimestampVector(key);
				TimestampVector entTimestampVector = tsMatrix.getTimestampVector(key);

				if (thisTimestampVector != null) {
					thisTimestampVector.updateMax(entTimestampVector);
				}
			}
		}
	}
	
	/**
	 * substitutes current timestamp vector of node for tsVector
	 * @param node
	 * @param tsVector
	 */
	public void update(String node, TimestampVector tsVector){
		synchronized (node) {
			timestampMatrix.put(node, tsVector);
		}
	}
	
	/**
	 * @return a timestamp vector containing, for each node, 
	 * the timestamp known by all participants
	 */
	public synchronized TimestampVector minTimestampVector(){
		
		TimestampVector result = null;    
		
		Enumeration<String> keys = timestampMatrix.keys();

		while (keys.hasMoreElements()) {
			TimestampVector currentNodeTimestamp = timestampMatrix.get(keys.nextElement()).clone();
			if (result == null) {
				result = currentNodeTimestamp;
			} else {
				result.mergeMin(currentNodeTimestamp);
			}
		}
		return result;
	}
	
	/**
	 * clone
	 */
	public synchronized TimestampMatrix clone(){

		return new TimestampMatrix (this.timestampMatrix);
        
	}
	
	/**
	 * equals
	 */
	@Override
	public synchronized boolean equals(Object obj) {
		
		// It's exact
		if (this == obj) {
			return true;
		}
		
		// It's null
		if (obj == null) {
			return false;
		}
		
		
		// Isn't same object type
		if (getClass() != obj.getClass()) {
			return false;
		}
		
		// Is possible to cast
		TimestampMatrix objectCast = (TimestampMatrix) obj;
		

		
		if (objectCast.timestampMatrix.size() == 0 && this.timestampMatrix.size() == 0) {
			return true;
		}
		
		return this.timestampMatrix.equals(objectCast.timestampMatrix);
	}

	
	/**
	 * toString
	 */
	@Override
	public synchronized String toString() {
		String all="";
		if (timestampMatrix == null){
			return all;
		}
		
		for(Enumeration<String> enumeration = timestampMatrix.keys(); enumeration.hasMoreElements();){
			
			String name = enumeration.nextElement();
			
			if(timestampMatrix.get(name) != null)
				all += name + " : " + timestampMatrix.get(name) + "\n";
		}
		return all;
	}
	

}