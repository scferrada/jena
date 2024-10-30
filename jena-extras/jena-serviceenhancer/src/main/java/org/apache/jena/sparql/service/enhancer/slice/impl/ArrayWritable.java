/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jena.sparql.service.enhancer.slice.impl;

import java.io.IOException;
import java.lang.reflect.Array;

import org.apache.jena.sparql.service.enhancer.slice.api.ArrayOps;
import org.apache.jena.sparql.service.enhancer.slice.api.HasArrayOps;

/** Interface for putting an array of items into a sequence at a certain offset */
public interface ArrayWritable<A>
    extends HasArrayOps<A>
{
    /** The method that needs to be implemented; all other methods default-delegate to this one. */
    void write(long offsetInBuffer, A arrayWithItemsOfTypeT, int arrOffset, int arrLength) throws IOException;

    default void put(long offset, Object item) throws IOException {
        ArrayOps<A> arrayOps = getArrayOps();
        A singleton = arrayOps.create(1);
        arrayOps.set(singleton, 0, item);
        write(offset, singleton);
    }

    default void write(long offset, A arrayWithItemsOfTypeT, int arrOffset) throws IOException {
        write(offset, arrayWithItemsOfTypeT, 0, Array.getLength(arrayWithItemsOfTypeT) - arrOffset);
    }

    default void write(long offset, A arrayWithItemsOfTypeT) throws IOException {
        write(offset, arrayWithItemsOfTypeT, 0, Array.getLength(arrayWithItemsOfTypeT));
    }
}
