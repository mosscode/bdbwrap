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

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;
import com.sleepycat.je.Transaction;

public abstract class SingleKeySecondaryDbWrap<K, V> extends SecondaryDbWrap<K, V>{
	final SecondaryKeyCreator keyCreator;
	
	private boolean immutableSecondaryKey = true;
	private boolean allowDuplicates = true;

	public SingleKeySecondaryDbWrap(String name, final DbWrap<?, V> primary, final Serializer<K> keySerializer) {
		super(name, primary, keySerializer);
		this.keyCreator = new SecondaryKeyCreator() {
			
			public boolean createSecondaryKey(SecondaryDatabase db,
					DatabaseEntry key, DatabaseEntry data, DatabaseEntry result)
					throws DatabaseException {
				V value = (V) primary.valueSerializer.deSerialize(data.getData());
				
				byte[] keyValue = createKey(value); 
				
				if (keyValue != null) {
					result.setData(keyValue);
					return true;
				}
				else {
					return false;
				}
			}
		};
	}
	
	public SingleKeySecondaryDbWrap<K, V> withImmutableSecondaryKey(boolean immutableSecondaryKey) {
		this.immutableSecondaryKey = immutableSecondaryKey;
		return this;
	}
	
	public SingleKeySecondaryDbWrap<K, V> withAllowDuplicates(boolean allowDuplicates) {
		this.allowDuplicates = allowDuplicates;
		return this;
	}
	
	public void visit(K key, ValueScanner<V> visitor, Transaction t) throws DatabaseException {
		final DatabaseEntry keyEntry = new DatabaseEntry();
		final DatabaseEntry valueEntry = new DatabaseEntry();
		
		final SecondaryCursor cursor = db.openSecondaryCursor(t, null);
		
		try {
			V value;
			final LockMode lockMode = LockMode.DEFAULT;
			for(
					OperationStatus status = cursor.getNext(keyEntry, valueEntry, lockMode);
					status != OperationStatus.SUCCESS; 
					status = cursor.getNext(keyEntry, valueEntry, lockMode)){
				value = primary.valueSerializer.deSerialize(valueEntry.getData());
				visitor.inspect(value);
			}
		}finally{
			cursor.close();
		}
	}
	
	final byte[] createKey(V data){
		K keyValue = extractKey(data);
		if (keyValue == null) {
			return null;
		}
		else {
			return writeKey(keyValue);
		}
	}
	
	public abstract K extractKey(V data);
	
	@Override
	public void init() throws DatabaseException {
		
		SecondaryConfig config = new SecondaryConfig();
		config.setAllowCreate(true);
		config.setAllowPopulate(true);
		config.setTransactional(true);
		config.setImmutableSecondaryKey(immutableSecondaryKey);
		config.setSortedDuplicates(allowDuplicates);
		config.setKeyCreator(keyCreator);
		
		db = primary.envWrap.env.openSecondaryDatabase(null, name, primary.db, config);
	}
	
}