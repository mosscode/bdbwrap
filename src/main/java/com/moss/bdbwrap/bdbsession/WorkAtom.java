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
package com.moss.bdbwrap.bdbsession;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.moss.bdbwrap.DbWrap.RetriesFailureException;
import com.moss.bdbwrap.EnvironmentWrap;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.Transaction;

public abstract class WorkAtom {
	
	private final Log log = LogFactory.getLog(this.getClass());
	
	private final EnvironmentWrap e;
	
	public WorkAtom(EnvironmentWrap e) {
		super();
		this.e = e;
	}
	
	public final void run() {
		Transaction tx = e.begin();
		try{
			doWork(tx);
			tx.commit();
		}catch(Throwable t){
			tx.abort();
			throw new RuntimeException(getClass().getSimpleName() + " aborted due to uncaught " + t.getClass().getSimpleName(), t);
		}
	}
	
	public final void runRetry() throws RetriesFailureException {
		runRetry(3);
	}
	
	public void runRetry(int attempts) throws RetriesFailureException {
		
		if (attempts < 0) {
			throw new RuntimeException("Number of attempts must be greater than zero.");
		}
		
		int remainingRetries = attempts;
		LockConflictException lockConflict = null;
		
		while (remainingRetries > 0) {
			
			Transaction tx = null;
			try {
				tx = e.begin();
				
				doWork(tx);
				
				tx.commit();
				tx = null;

				lockConflict = null;
			}
			catch (LockConflictException ex) {
				if (tx != null) {
					try {
						tx.abort();
					}
					catch (Throwable e) {
						log.error("Failed to abort tx", e);
					}
				}
				lockConflict = ex;
			}
			catch (Throwable ex) {
				if (tx != null) {
					try {
						tx.abort();
					}
					catch (Throwable e) {
						log.error("Failed to abort tx", e);
					}
				}
				throw new RuntimeException(ex);
			}
			
			if (lockConflict == null) {
				return;
			}
			
			remainingRetries--;
			log.warn(getClass().getSimpleName() + " aborted due to lock conflict " + lockConflict.getClass().getSimpleName() + ", will retry (attempt " + (attempts - remainingRetries + 1) + ")", lockConflict);
		}
		
		throw new RuntimeException(getClass().getSimpleName() + " aborted due to lock conflict " + lockConflict.getClass().getSimpleName() + ", failed after " + attempts + " attempts.", lockConflict);
	}
	
	public WorkAtom add(final WorkAtom other){
		final WorkAtom thisAtom = this;
		
		if(other.e!=e){
			throw new RuntimeException("Cannot merge work atoms because they correspond to different environments");
		}
		
		return new WorkAtom(e){
			@Override
			protected void doWork(Transaction tx) throws Exception {
				other.doWork(tx);
				thisAtom.doWork(tx);
			}
		};
	}
	protected abstract void doWork(Transaction tx) throws Exception ;
}
