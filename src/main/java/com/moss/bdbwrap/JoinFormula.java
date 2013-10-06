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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.Transaction;

public class JoinFormula <V>{
	private DbWrap<?, V> primary;
	private List<JoinCriteria<?, V>> criteria = new LinkedList<JoinCriteria<?, V>>();
	
	public JoinFormula(DbWrap<?, V> primary) {
		super();
		this.primary = primary;
	}
	
	
	public <K> JoinFormula<V> joinWith(SecondaryDbWrap<K, V> secondary, K position){
		criteria.add(new JoinCriteria<K, V>(secondary, position));
		return this;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked", "deprecation" })
	public Join<V> create(Transaction t, LockMode mode) throws DatabaseException {
		List<SecondaryCursor> cursors = new LinkedList<SecondaryCursor>();
		
		boolean validJoin = true;
		
		for(JoinCriteria next:criteria){
			SecondaryCursor c = next.secondary.openCursor(t);
			cursors.add(c);
			DatabaseEntry key = new DatabaseEntry(next.secondary.writeKey(next.position));
			DatabaseEntry foundData = new DatabaseEntry();
			OperationStatus status = c.getSearchKey(key, foundData, mode);
			if(status != OperationStatus.SUCCESS){
				validJoin = false;
				break;
			}
				//throw new DatabaseException("Unable to position cursor on '" + next.secondary.name + "' at position '" + next.position + "'");
		}
		
		if(!validJoin){
			for (SecondaryCursor next : cursors) {
				next.close();
			}
			return null;
		}
		
		return new Join<V>(primary.valueSerializer, cursors, primary.db.join(cursors.toArray(new Cursor[cursors.size()]), null));
	}
	
	public List<JoinCriteria<?, V>> criteria() {
		return new ArrayList<JoinCriteria<?, V>>(criteria);
	}
}