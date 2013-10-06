/**
 * Copyright (C) 2013, Moss Computing Inc.
 *
 * This file is part of bdbwrap.
 *
 * bdbwrap is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2, or (at your option)
 * any later version.
 *
 * bdbwrap is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with bdbwrap; see the file COPYING.  If not, write to the
 * Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 *
 * Linking this library statically or dynamically with other modules is
 * making a combined work based on this library.  Thus, the terms and
 * conditions of the GNU General Public License cover the whole
 * combination.
 *
 * As a special exception, the copyright holders of this library give you
 * permission to link this library with independent modules to produce an
 * executable, regardless of the license terms of these independent
 * modules, and to copy and distribute the resulting executable under
 * terms of your choice, provided that you also meet, for each linked
 * independent module, the terms and conditions of the license of that
 * module.  An independent module is a module which is not derived from
 * or based on this library.  If you modify this library, you may extend
 * this exception to your version of the library, but you are not
 * obligated to do so.  If you do not wish to do so, delete this
 * exception statement from your version.
 */
/**
 * 
 */
package com.moss.bdbwrap;

import java.util.Collections;
import java.util.List;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.JoinCursor;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryCursor;

public class Join <V> {
	private final List<SecondaryCursor> secondaryCursors;
	private final JoinCursor joinCursor;
	private final Serializer<V> serializer;
	
	public Join(Serializer<V> serializer, List<SecondaryCursor> secondaryCursors,
			JoinCursor joinCursor) {
		super();
		this.serializer = serializer;
		this.secondaryCursors = secondaryCursors;
		this.joinCursor = joinCursor;
	}
	
	public void close() throws DatabaseException {
		joinCursor.close();
		for(SecondaryCursor next:secondaryCursors){
			next.close();
		}
	}
	
	public JoinCursor getJoinCursor() {
		return joinCursor;
	}
	
	public List<SecondaryCursor> getSecondaryCursors() {
		return Collections.unmodifiableList(secondaryCursors);
	}
	
	public void scan(ValueScanner<V> scanner) {
		JoinCursor joinCursor = getJoinCursor();

		try {
			DatabaseEntry foundKey = new DatabaseEntry();
			DatabaseEntry foundData = new DatabaseEntry();

			while (joinCursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
				scanner.inspect(serializer.deSerialize(foundData.getData()));
			}
		} finally {
			close();
		}
		
	}
}