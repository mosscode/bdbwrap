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
package com.moss.bdbwrap.tostring;

import java.io.UnsupportedEncodingException;

import com.moss.bdbwrap.Serializer;

public class ToStringSerializer <T> implements Serializer<T> {
	private static final String NULL_KEY_VALUE = "null";
	
	private final String encoding;
	private final FromStringFactory<T> factory;
	
	public ToStringSerializer(final FromStringFactory<T> factory) {
		this(factory, "UTF8");
	}
	
	public ToStringSerializer(final FromStringFactory<T> factory, final String encoding) {
		super();
		this.encoding = encoding;
		this.factory = factory;
	}

	public T deSerialize(byte[] data) {
		try {
			if(data==null) return null;
			else {
				String text = new String(data, encoding);
				if(text.equals(NULL_KEY_VALUE)){
					return null;
				}else{
					return factory.fromString(text);
				}
			}
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}
	
	public byte[] serialize(T obj) {
		try {
			if(obj==null) return NULL_KEY_VALUE.getBytes(encoding);
			else return obj.toString().getBytes(encoding);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	};
}
