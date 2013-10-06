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

import java.util.List;
import java.util.Set;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryMultiKeyCreator;

public abstract class MultiKeySecondaryDbWrap<K, V> extends SecondaryDbWrap<K, V>{
	final SecondaryMultiKeyCreator keyCreator;
	
	private boolean immutableSecondaryKey = true;
	private boolean sortedDuplicates = true;

	public MultiKeySecondaryDbWrap(String name, final DbWrap<?, V> primary, final Serializer<K> keySerializer) {
		super(name, primary, keySerializer);
		this.keyCreator = new SecondaryMultiKeyCreator() {
			
			public void createSecondaryKeys(SecondaryDatabase db, DatabaseEntry key, DatabaseEntry data, Set<DatabaseEntry> result)
					throws DatabaseException {
				
				V value = primary.valueSerializer.deSerialize(data.getData());
				
				for(K nextKey: createKeys(value)){
					result.add(new DatabaseEntry(keySerializer.serialize(nextKey)));
				}
				
			}
		};;;
	}
	
	public final MultiKeySecondaryDbWrap<K, V> withImmutableSecondaryKey(boolean immutableSecondaryKey) {
		this.immutableSecondaryKey = immutableSecondaryKey;
		return this;
	}
	
	public final MultiKeySecondaryDbWrap<K, V> withSortedDuplicates(boolean sortedDuplicates) {
		this.sortedDuplicates = sortedDuplicates;
		return this;
	}

	protected abstract List<K> createKeys(V data);
	
	@Override
	public void init() throws DatabaseException {
		
		SecondaryConfig config = new SecondaryConfig();
		config.setAllowCreate(true);
		config.setAllowPopulate(true);
		config.setTransactional(true);
		config.setImmutableSecondaryKey(immutableSecondaryKey);
		config.setSortedDuplicates(sortedDuplicates);
		
		config.setMultiKeyCreator(keyCreator);
		
		db = primary.envWrap.env.openSecondaryDatabase(null, name, primary.db, config);
	}
}