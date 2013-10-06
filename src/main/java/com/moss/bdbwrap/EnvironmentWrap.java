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

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.je.Transaction;

public abstract class EnvironmentWrap {
	private final Log log = LogFactory.getLog(this.getClass());
	
	protected final List<DbWrap<?,?>> databases = new LinkedList<DbWrap<?,?>>();
	public Environment env;
	private final File envDir;
	private final Long sharedCacheSize;
	
	public EnvironmentWrap(File envDir, Long sharedCacheSize) {
		super();
		this.envDir = envDir;
		this.sharedCacheSize = sharedCacheSize;
	}


	public void load(){
			
			if (log.isDebugEnabled()) {
				log.debug("Loading environment using envDir: " + envDir);
			}
			
			if (!envDir.exists() && !envDir.mkdirs()) {
				throw new RuntimeException("Cannot create envDir: " + envDir);
			}
			
			try {
				
				if (log.isDebugEnabled()) {
					log.debug("Opening environment");
				}
				
				env = initEnvironment();
				
				if (log.isDebugEnabled()) {
					log.debug("Opening databases");
				}
				
				{
					
				}
				{
					// Little sanity check here to discover duplicate database names.
					// this usually ends up being the result of copy+paste errors
					Set<String> names = new TreeSet<String>();
					
					for (DbWrap<?, ?> other : databases) {
						if(names.contains(other.name)){
							throw new RuntimeException("There are two databases with the same name (\"" + other.name + "\")");
						}else{
							names.add(other.name);
						}
						
						for(SecondaryDbWrap<?, ?> secondary : other.secondaries){
							if(names.contains(secondary.name)){
								throw new RuntimeException("There are two databases with the same name (\"" + secondary.name + "\")");
							}else{
								names.add(secondary.name);
							}
						}
					}
				}
				
				for (DbWrap<?, ?> next : databases) {
					if(!next.hasInitialized()){
						try {
							next.init();
						} catch (Exception e) {
							throw new RuntimeException("Error initializing " + next.name + " at " + envDir.getAbsolutePath(), e);
						}
						for(SecondaryDbWrap<?, ?> nextSecondary: next.secondaries){
							nextSecondary.init();
						}
					}
				}
			}
			catch (Exception ex) {
				throw new RuntimeException(ex);
			}
	}
	
	protected Environment initEnvironment() throws EnvironmentLockedException, DatabaseException {
		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setAllowCreate(true);
		envConfig.setTransactional(true);
		envConfig.setSharedCache(true);
		envConfig.setCacheSize(sharedCacheSize);
		
		return new Environment(envDir, envConfig);
	}

	public Transaction begin(){
		try {
			return env.beginTransaction(null, null);
		}
		catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
	public Transaction begin(Transaction t){
		try {
			return env.beginTransaction(t, null);
		}
		catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
	public void close() {
		
		boolean closeFailed = false;
		
		for(DbWrap<?, ?> next : databases){
			try {
				if (log.isDebugEnabled()) {
					log.debug("Closing database " + next.name);
				}
				for(SecondaryDbWrap<?, ?> nextSecondary: next.secondaries){
					nextSecondary.db.close();
				}
				next.db.close();
			}
			catch (DatabaseException ex) {
				closeFailed = true;
				if (log.isErrorEnabled()) {
					log.error("Could not close " + next.name + " db", ex);
				}
			}
		}
		
		
		try {
			env.close();
		}
		catch (DatabaseException ex) {
			closeFailed = true;
			if (log.isErrorEnabled()) {
				log.error("Could not close environment", ex);
			}
		}
		
		if (closeFailed) {
			throw new RuntimeException("Failed to close store persistence context, see logs for more info");
		}
	}
	
	public final List<DbWrap<?, ?>> databases() {
		return new ArrayList<DbWrap<?,?>>(databases);
	}
}
