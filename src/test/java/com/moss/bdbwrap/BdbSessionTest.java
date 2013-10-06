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

import junit.framework.TestCase;

import com.moss.bdbwrap.bdbsession.BdbSession;
import com.sleepycat.je.Transaction;

public class BdbSessionTest extends TestCase {
	
	static class Bomb {
		public void ignite() throws ExplosionException {
			throw new ExplosionException("Boom!");
		}
		public void shake() {
			throw new RuntimeException("Ka-Boom! (unexpected explosion)");
		}
		public String defuse(){
			return "tik...tok......tik......[silence]";
		}
	}
	
	@SuppressWarnings("serial")
	static class ExplosionException extends Exception {

		public ExplosionException(String message) {
			super(message);
		}
		
	}
	
	public void testNormalReturn(){
		TestEnvironmentWrap p = new TestEnvironmentWrap(TempDir.create());
		
		Transaction t = p.begin();
		
		BdbSession<TestEnvironmentWrap> session = new BdbSession<TestEnvironmentWrap>(t, p);
		
		try {
			session.wrap(Bomb.class).defuse();
			session.commit();
		} catch (Throwable e) {
			e.printStackTrace();
			fail("Something went is wrong");
		}
//		try {
//			t.abort();
//			fail("Should have got an exception here since the transaction was already committed");
//		} catch (Exception e1) {
//			// expected
//		}
	}
	
	public void testUnCheckedException(){
		TestEnvironmentWrap p = new TestEnvironmentWrap(TempDir.create());
		
		Transaction t = p.begin();
		
		BdbSession<TestEnvironmentWrap> session = new BdbSession<TestEnvironmentWrap>(t, p);
		
		try {
			session.wrap(Bomb.class).shake();
			fail("Should have caught an " + ExplosionException.class.getSimpleName() + " at this point");
		} catch (RuntimeException e) {
			try {
				t.commit();
				fail("Should have got an exception here since the transaction was already aborted");
			} catch (Exception e1) {
				// expected
			}
		}
	}
	public void testCheckedException(){
		TestEnvironmentWrap p = new TestEnvironmentWrap(TempDir.create());
		
		Transaction t = p.begin();
		
		BdbSession<TestEnvironmentWrap> session = new BdbSession<TestEnvironmentWrap>(t, p);
		
		try {
			session.wrap(Bomb.class).ignite();
			fail("Should have caught an " + ExplosionException.class.getSimpleName() + " at this point");
		} catch (ExplosionException e) {
			try {
				t.commit();
				fail("Should have got an exception here since the transaction was already aborted");
			} catch (Exception e1) {
				// expected
			}
		}
	}
}
