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

import com.sleepycat.je.Cursor;

public interface ValueWorker <T> {
	public enum UdOp {UPDATE, DELETE, NOTHING;}
	
	public static class Result <T>{
		public static <T> Result<T> delete(){
			return new Result<T>(UdOp.DELETE, null);
		}
		public static <T> Result<T> nothing(){
			return new Result<T>(UdOp.NOTHING, null);
		}
		public static <T> Result<T> update(T value){
			return new Result<T>(UdOp.UPDATE, value);
		}
		final UdOp op;
		final T value;
		
		public Result(UdOp op, T value) {
			super();
			this.op = op;
			this.value = value;
		}
		
	}
	
	Result<T> next(T value, Cursor cursor);
}
