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
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.moss.bdbwrap.bdbsession.WorkAtom;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

public class DbWrap<K, V> {
		private final Log log = LogFactory.getLog(getClass());
		
		private final List<PutHook<K, V>> hooks = new ArrayList<PutHook<K,V>>();
		private final List<PutInterceptHook<K, V>> intercepts = new ArrayList<PutInterceptHook<K, V>>();
		private boolean permitDuplicates = new DatabaseConfig().getSortedDuplicates();
		
		public final EnvironmentWrap envWrap;
		public final String name;
		public final List<SecondaryDbWrap<?, V>> secondaries = new LinkedList<SecondaryDbWrap<?, V>>();
		public final Serializer<K> keySerializer;
		public final Serializer<V> valueSerializer;
		
		public Database db;
		
		public DbWrap(String name, Serializer<K> keySerializer, Serializer<V> valueSerializer, final EnvironmentWrap environment, PutHook<K, V> ... hooks) {
			super();
			this.name = name;
			this.envWrap = environment;
			environment.databases.add(this);
			this.keySerializer = keySerializer;
			this.valueSerializer = valueSerializer;
			if(hooks!=null){
				this.hooks.addAll(Arrays.asList(hooks));
			}
		}
		
		public DbWrap<K, V> withHook(PutHook<K, V> hook){
			if(db!=null) throw new RuntimeException("I won't let you add a hook on-the-fly to an open database.");
			hooks.add(hook);
			return this;
		}
		
		public DbWrap<K, V> withHook(PutInterceptHook<K, V> hook){
			if(db!=null) throw new RuntimeException("I won't let you add a hook on-the-fly to an open database.");
			intercepts.add(hook);
			return this;
		}
		
		
		boolean hasInitialized(){
			return db!=null;
		}
		protected void init() throws DatabaseException {
			if (log.isDebugEnabled()) {
				log.debug("Opening '" + name + "' database");
			}
			
			DatabaseConfig dbConfig = new DatabaseConfig();
			dbConfig.setAllowCreate(true);
			dbConfig.setTransactional(true);
			dbConfig.setSortedDuplicates(permitDuplicates);
			
			db = envWrap.env.openDatabase(null, name, dbConfig);
		}
		
		public void delete(K key, Transaction t) throws DatabaseException {

			DatabaseEntry keyEntry;
			keyEntry = new DatabaseEntry(keySerializer.serialize(key));
			db.delete(t, keyEntry);
		}
		
		/**
		 * @deprecated use scan() instead
		 */
		@Deprecated
		public void visit(final ValueVisitor<V> visitor, Transaction t) {
			scan(
				new ValueScanner<V>() {
					public void inspect(V next) {
						visitor.visit(next);
					};
				}, 
				t
			);
		}
		
		public void runJob(final ValueWorker<V> worker){
			new WorkAtom(this.envWrap) {
				
				@Override
				protected void doWork(Transaction tx) throws Exception {

					Cursor loopCursor = null;
					
					try {
//						TransactionConfig tConfig = new TransactionConfig();
//						t = envWrap.env.beginTransaction(null, tConfig);
						
						CursorConfig cursorConfig = new CursorConfig();
						loopCursor = db.openCursor(tx, cursorConfig);
						
						DatabaseEntry key = new DatabaseEntry();
						DatabaseEntry data = new DatabaseEntry();
						
						while (OperationStatus.SUCCESS == loopCursor.getNext(key, data, null)) {
							
							V value = valueSerializer.deSerialize(data.getData());//read(type, data);
							ValueWorker.Result<V> result = worker.next(value, loopCursor);
							switch(result.op){
							case DELETE:
								loopCursor.delete();
								break;
							case UPDATE:
								data.setData(valueSerializer.serialize(result.value));
								loopCursor.putCurrent(data);
								break;
							case NOTHING:
								break;
							default: throw new RuntimeException("Unknown result op: " + result.op);
							}
						}
						
						loopCursor.close();
						loopCursor = null;
						
//						t.commit();
//						t = null;
					}
					catch (Throwable ex) {
						
						try {
							if (loopCursor != null) {
								loopCursor.close();
							}

//							if (t != null) {
//								t.abort();
//							}
						}
						catch (Exception e) {
							ex.printStackTrace();
						}
						
						throw new RuntimeException(ex);
					}
				}
			}.run();
		}
		
		public void scan(ValueScanner<V> visitor, Transaction t) {
			
			Cursor loopCursor = null;
			
			try {
				CursorConfig cursorConfig = new CursorConfig();
				loopCursor = db.openCursor(t, cursorConfig);
				
				DatabaseEntry key = new DatabaseEntry();
				DatabaseEntry data = new DatabaseEntry();
				
				while (OperationStatus.SUCCESS == loopCursor.getNext(key, data, null)) {
					
					V expense = valueSerializer.deSerialize(data.getData());//read(type, data);
					visitor.inspect(expense);
				}
				
				loopCursor.close();
				loopCursor = null;
			}
			catch (Throwable ex) {
				
				try {
					if (loopCursor != null) {
						loopCursor.close();
					}
				}
				catch (Exception e) {
					ex.printStackTrace();
				}
				
				throw new RuntimeException(ex);
			}
		}
		
		public void scanUncommitted(ValueScanner<V> visitor) {
			
			Cursor loopCursor = null;
			
			try {
				CursorConfig cursorConfig = new CursorConfig();
				cursorConfig.setReadUncommitted(true);
				loopCursor = db.openCursor(null, cursorConfig);
				
				DatabaseEntry key = new DatabaseEntry();
				DatabaseEntry data = new DatabaseEntry();
				
				while (OperationStatus.SUCCESS == loopCursor.getNext(key, data, null)) {
					
					V value = valueSerializer.deSerialize(data.getData());
					visitor.inspect(value);
				}
				
				loopCursor.close();
				loopCursor = null;
			}
			catch (Throwable ex) {
				
				try {
					if (loopCursor != null) {
						loopCursor.close();
					}
				}
				catch (Exception e) {
					ex.printStackTrace();
				}
				
				throw new RuntimeException(ex);
			}
		}
		
		public void search(SearchVisitor<K, V> visitor) {
			
			Cursor loopCursor = null;
			
			try {
				CursorConfig cursorConfig = new CursorConfig();
				loopCursor = db.openCursor(null, cursorConfig);
				
				DatabaseEntry key = new DatabaseEntry();
				DatabaseEntry data = new DatabaseEntry();
				
				while (OperationStatus.SUCCESS == loopCursor.getNext(key, data, null)) {
					K k = keySerializer.deSerialize(key.getData());
					V v = valueSerializer.deSerialize(data.getData());//read(type, data);
					
					boolean continueSearch = visitor.next(k, v);
					
					if (!continueSearch) {
						break;
					}
				}
				
				loopCursor.close();
				loopCursor = null;
			}
			catch (Throwable ex) {
				
				try {
					if (loopCursor != null) {
						loopCursor.close();
					}
				}
				catch (Exception e) {
					ex.printStackTrace();
				}
				
				throw new RuntimeException(ex);
			}
		}
		
		public void clear() throws DatabaseException {
			for(SecondaryDbWrap<?, V> next : secondaries){
				next.db.close();
			}
			db.close();
			
			envWrap.env.truncateDatabase(null, name, false);
			for(SecondaryDbWrap<?, V> next : secondaries){
				envWrap.env.truncateDatabase(null, next.name, false);
			}
			init();
			for(SecondaryDbWrap<?, V> next : secondaries){
				next.init();
			}

		}
		
		public void put(K key, V value, Transaction t){
			if (log.isDebugEnabled()) {
				log.debug("Persisting " + name);
			}
			
			try {
				for(PutInterceptHook<K, V> hook : intercepts){
					hook.intercept(key, value);
				}
				DatabaseEntry keyEntry = new DatabaseEntry(keySerializer.serialize(key));
				DatabaseEntry dataEntry = new DatabaseEntry(valueSerializer.serialize(value));
				db.put(t, keyEntry, dataEntry);
				for(PutHook<K, V> hook : hooks){
					hook.putHappened(key, keyEntry, value, dataEntry, this);
				}
			}
			catch (Exception ex) {
				throw new RuntimeException(ex);
			}
			
		}
		
		public void copyFrom(K key, DbWrap<K, V> from, Transaction t) {
			
			if (log.isDebugEnabled()) {
				log.debug("Copying entry from " + from.name + " to "+ name + " (" + key + ")");
			}
			
			DatabaseEntry k = new DatabaseEntry(keySerializer.serialize(key));
			DatabaseEntry v = new DatabaseEntry();
			OperationStatus status = from.db.get(t, k, v, LockMode.READ_COMMITTED);
			if (status != OperationStatus.SUCCESS) {
				throw new RuntimeException("Could not find value for copy: " + key);
			}
			db.put(t, k, v);
		}
		
		public void moveFrom(K key, DbWrap<K, V> from, Transaction t) {
			
			if (log.isDebugEnabled()) {
				log.debug("Moving entry from " + from.name + " to "+ name + " (" + key + ")");
			}
			
			DatabaseEntry k = new DatabaseEntry(keySerializer.serialize(key));
			DatabaseEntry v = new DatabaseEntry();
			OperationStatus status = from.db.get(t, k, v, LockMode.READ_COMMITTED);
			if (status != OperationStatus.SUCCESS) {
				throw new RuntimeException("Could not find value for copy: " + key);
			}
			from.db.delete(t, k);
			db.put(t, k, v);
		}
		
		public OperationStatus putNoOverwrite(K key, V value, Transaction t){
			if (log.isDebugEnabled()) {
				log.debug("Persisting " + name);
			}
			
			try {
				for(PutInterceptHook<K, V> hook : intercepts){
					hook.intercept(key, value);
				}
				DatabaseEntry keyEntry = new DatabaseEntry(keySerializer.serialize(key));
				DatabaseEntry dataEntry = new DatabaseEntry(valueSerializer.serialize(value));
				OperationStatus status = db.putNoOverwrite(t, keyEntry, dataEntry);
				if (OperationStatus.SUCCESS == status) {
					for(PutHook<K, V> hook : hooks){
						hook.putHappened(key, keyEntry, value, dataEntry, this);
					}
				}
				return status;
			}
			catch (Exception ex) {
				throw new RuntimeException(ex);
			}
			
		}
		
		public V get(K keyd, Transaction t, LockMode mode){
			try {
				DatabaseEntry key = new DatabaseEntry(keySerializer.serialize(keyd));
				DatabaseEntry data = new DatabaseEntry();
				
				if (OperationStatus.SUCCESS == db.get(t, key, data, mode)) {
					return valueSerializer.deSerialize(data.getData());
				}
				else {
					return null;
				}
			}
			catch (Exception ex) {
				throw new RuntimeException(ex);
			}
		}
		
		public V getRequired(K keyd, Transaction t, LockMode mode){
			try {
				DatabaseEntry key = new DatabaseEntry(keySerializer.serialize(keyd));
				DatabaseEntry data = new DatabaseEntry();
				
				if (OperationStatus.SUCCESS == db.get(t, key, data, mode)) {
					return valueSerializer.deSerialize(data.getData());
				}
				else {
					throw new RuntimeException("Cannot find value for key: " + keyd);
				}
			}
			catch (Exception ex) {
				throw new RuntimeException(ex);
			}
		}
		
		public <SK> JoinFormula<V> joinWith(SecondaryDbWrap<SK, V> secondary, SK position){
			return new JoinFormula<V>(this).joinWith(secondary, position);
		}
		
		@Deprecated
		public V deserialize(byte[] value){
			return valueSerializer.deSerialize(value);
		}
		
		@Deprecated
		byte[] serializeKey(K key){
			return keySerializer.serialize(key);
		}
		
		public DbWrap<K, V> permitDuplicateKeys() {
			permitDuplicates = true;
			return this;
		}
		
		public final class PreSerializingPut {
			private final DatabaseEntry key, value;

			public PreSerializingPut(DatabaseEntry key, DatabaseEntry value) {
				this.key = key;
				this.value = value;
			}

			public void put(Transaction t) {
				db.put(t, key, value);
			}
			
			public void putNoOverwrite(Transaction t) {
				db.putNoOverwrite(t, key, value);
			}
			
			public void putRetry() throws RetriesFailureException {
				putRetry(3);
			}
			
			public void putRetry(int attempts) throws RetriesFailureException {
				
				if (attempts < 0) {
					throw new RuntimeException("Number of attempts must be greater than zero.");
				}
				
				Throwable fail = null;
				
				for (int i=0; i<attempts; i++) {
					if (fail != null) {
						log.warn("Failed to put() for database " + db.getDatabaseName() + ", will retry (attempt " + (i + 1) + ")", fail);
					}
					try {
						put(null);
						fail = null;
						break;
					}
					catch (Throwable ex) {
						fail = ex;
					}
				}
				
				if (fail != null) {
					throw new RetriesFailureException("Failed to put() for database " + db.getDatabaseName() + ", after " + attempts + " attempts, giving up.", fail);
				}
			}
			
			public WorkAtom toAtom() {
				return new WorkAtom(envWrap) {
					@Override
					protected void doWork(Transaction tx) throws Exception {
						put(tx);
					}
				};
			}
		}
		
		@SuppressWarnings("serial")
		public static final class RetriesFailureException extends RuntimeException {
			public RetriesFailureException(String message, Throwable cause) {
				super(message, cause);
			}
		}
		
		public PreSerializingPut prePut(K k, V v) {
			DatabaseEntry key = new DatabaseEntry(keySerializer.serialize(k));
			DatabaseEntry value = new DatabaseEntry(valueSerializer.serialize(v));
			return new PreSerializingPut(key, value);
		}
	}
