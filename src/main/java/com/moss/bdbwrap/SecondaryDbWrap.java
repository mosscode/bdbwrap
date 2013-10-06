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
package com.moss.bdbwrap;

import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.Transaction;

public abstract class SecondaryDbWrap<K, V> {
	
	public final String name;
	public final DbWrap<?, V> primary;
	public SecondaryDatabase db;
	protected final Serializer<K> keySerializer;
	
	public SecondaryDbWrap(String name, DbWrap<?, V> primary, final Serializer<K> keySerializer) {
		super();
		this.name = name;
		this.primary = primary;
		primary.secondaries.add(this);
		this.keySerializer = keySerializer;
	}
	
	public Serializer<K> keySerializer() {
		return keySerializer;
	}
	
	
	public SecondaryCursor openCursor(Transaction t) throws DatabaseException {
		return db.openCursor(t, null);
	}
	
	public SecondaryCursor openCursor(Transaction t, CursorConfig cursorConfig) throws DatabaseException {
		return db.openCursor(t, cursorConfig);
	}
	
	public V get(K keyd, Transaction t, LockMode mode){
		
		try {
			DatabaseEntry key = new DatabaseEntry(keySerializer.serialize(keyd));
			DatabaseEntry data = new DatabaseEntry();
			
			if (OperationStatus.SUCCESS == db.get(t, key, data, mode)) {
				return primary.valueSerializer.deSerialize(data.getData());//read(type, data);
			}
			else {
				return null;
			}
		}
		catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
	
	@Deprecated
	public byte[] writeKey(K key){
		return keySerializer.serialize(key);
	}
	public abstract void init() throws DatabaseException ;
	
	public void keySearchBackward(K searchKey, SearchVisitor<K, V> visitor, Transaction t) throws DatabaseException {
		final DatabaseEntry keyEntry = new DatabaseEntry();
		final DatabaseEntry valueEntry = new DatabaseEntry();
		
		final SecondaryCursor cursor = db.openSecondaryCursor(t, null);
		
		try {
			boolean keepSearching = true;
			K key;
			V value;
			final LockMode lockMode = LockMode.DEFAULT;
			keyEntry.setData(keySerializer.serialize(searchKey));
			for(
					OperationStatus status = cursor.getSearchKeyRange(keyEntry, valueEntry, lockMode);
					keepSearching && status != OperationStatus.NOTFOUND; 
					status = cursor.getPrev(keyEntry, valueEntry, lockMode)){
				key = keySerializer.deSerialize(keyEntry.getData());
				value = primary.valueSerializer.deSerialize(valueEntry.getData());
				keepSearching = visitor.next(key, value);
			}
		}finally{
			cursor.close();
		}
	}
	
	public void keySearchForward(K searchKey, SearchVisitor<K, V> visitor, Transaction t) throws DatabaseException {
		final DatabaseEntry keyEntry = new DatabaseEntry();
		final DatabaseEntry valueEntry = new DatabaseEntry();
		
		final SecondaryCursor cursor = db.openSecondaryCursor(t, null);
		
		try {
			boolean keepSearching = true;
			K key;
			V value;
			final LockMode lockMode = LockMode.DEFAULT;
			keyEntry.setData(keySerializer.serialize(searchKey));
			for(
					OperationStatus status = cursor.getSearchKeyRange(keyEntry, valueEntry, lockMode);
					keepSearching && status != OperationStatus.NOTFOUND; 
					status = cursor.getNext(keyEntry, valueEntry, lockMode)){
				key = keySerializer.deSerialize(keyEntry.getData());
				value = primary.valueSerializer.deSerialize(valueEntry.getData());
				keepSearching = visitor.next(key, value);
			}
		}finally{
			cursor.close();
		}
	}
	
	public void keySearchForwardPartial(byte[] partialSearchKey, SearchVisitor<K, V> visitor, Transaction t) throws DatabaseException {
		keySearchForwardPartial(partialSearchKey, visitor, t, null);
	}
	
	public void keySearchForwardPartial(byte[] partialSearchKey, SearchVisitor<K, V> visitor, Transaction t, CursorConfig cursorConfig) throws DatabaseException {
		final DatabaseEntry keyEntry = new DatabaseEntry(partialSearchKey);
		final DatabaseEntry valueEntry = new DatabaseEntry();
		
		final SecondaryCursor cursor = db.openSecondaryCursor(t, cursorConfig);
		
		try {
			boolean keepSearching = true;
			K key;
			V value;
			final LockMode lockMode = LockMode.DEFAULT;
			for(
					OperationStatus status = cursor.getSearchKeyRange(keyEntry, valueEntry, lockMode);
					keepSearching && status != OperationStatus.NOTFOUND; 
					status = cursor.getNext(keyEntry, valueEntry, lockMode)){
				key = keySerializer.deSerialize(keyEntry.getData());
				value = primary.valueSerializer.deSerialize(valueEntry.getData());
				keepSearching = visitor.next(key, value);
			}
		}finally{
			cursor.close();
		}
	}
	
//	public void scanPrimary(ValueScanner<V> visitor, Transaction t) {
//		
//		Cursor loopCursor = null;
//		
//		try {
//			CursorConfig cursorConfig = new CursorConfig();
//			loopCursor = db.openCursor(t, cursorConfig);
//			
//			DatabaseEntry key = new DatabaseEntry();
//			DatabaseEntry keyData = new DatabaseEntry();
//			
//			while (OperationStatus.SUCCESS == loopCursor.getNext(key, data, null)) {
//				V primaryKey = keySerializer.deSerialize(keyData.getData());
//				V expense = primary.valueSerializer.deSerialize(data.getData());//read(type, data);
//				primary.get(keyd, t, mode)
//				visitor.inspect(expense);
//			}
//			
//			loopCursor.close();
//			loopCursor = null;
//		}
//		catch (Throwable ex) {
//			
//			try {
//				if (loopCursor != null) {
//					loopCursor.close();
//				}
//			}
//			catch (Exception e) {
//				ex.printStackTrace();
//			}
//			
//			throw new RuntimeException(ex);
//		}
//	}
	

//	public void clear() throws DatabaseException {
//		primary.envWrap.env.truncateDatabase(null, name, false);
//	}
	
}