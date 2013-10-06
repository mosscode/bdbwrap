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

import java.lang.reflect.Method;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.moss.bdbwrap.EnvironmentWrap;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Transaction;

public class BdbSession <E extends EnvironmentWrap> {
	private final Transaction t;
	private final E data;
	private final Log log = LogFactory.getLog(getClass());
	
	public BdbSession(Transaction t, E environment) {
		super();
		this.t = t;
		this.data = environment;
	}
	
	@SuppressWarnings("unchecked")
	public <T> T wrap(Class<T> clazz){
		T obj = (T) Enhancer.create(clazz, new CglibInterceptor());
		
		return obj;
	}
	
	@SuppressWarnings("unchecked")
	public <T extends BdbSessionAction<E>> T wrapAction(Class<T> clazz){
		T obj;
		try {
			obj = (T) Enhancer.create(clazz, new CglibInterceptor());
		} catch (Exception e) {
			throw new RuntimeException("Error creating decorated version of class '" + clazz.getSimpleName() + "': " + e.getMessage(), e);
		}
		
		obj.setEnvironment(data);
		obj.setTransaction(t);
		
		return obj;
	}
	
	private class CglibInterceptor implements MethodInterceptor {
		public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
			if(log.isDebugEnabled()) log.debug("Calling " + method);
			try {
				return proxy.invokeSuper(obj, args);
			} catch (Throwable e) {
				if(log.isDebugEnabled())
					log.debug("Aborting session due to thrown exception; " + e.getClass().getSimpleName(), e);
				abort();
				throw e;
			}
		}
		
	}
	
	public Transaction transaction() {
		return t;
	}
	
	public void abort(){
		try {
			t.abort();
		} catch (DatabaseException e) {
			throw new RuntimeException(e);
		}
	}
	
	public void commit(){
		try {
			t.commit();
		} catch (DatabaseException e) {
			throw new RuntimeException(e);
		}
	}
}
