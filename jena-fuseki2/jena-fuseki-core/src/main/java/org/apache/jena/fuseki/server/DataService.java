/**
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

package org.apache.jena.fuseki.server;

import static java.lang.String.format;
import static org.apache.jena.fuseki.server.DataServiceStatus.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import org.apache.jena.fuseki.FusekiException;
import org.apache.jena.fuseki.auth.AuthPolicy;
import org.apache.jena.query.TxnType;
import org.apache.jena.query.text.DatasetGraphText;
import org.apache.jena.sparql.core.DatasetGraph;

public class DataService {
    private DatasetGraph dataset;
    
    private EndpointSet unnamedEndpoints                      = new EndpointSet(null);
    private Map<String, EndpointSet> namedEndpoints           = new ConcurrentHashMap<>();
    private Map<String, Endpoint> endpoints                   = new HashMap<>();
    // Keep a single multimap of operation->endpoints. 
    private ListMultimap<Operation, Endpoint> operationsMap   = Multimaps.synchronizedListMultimap(ArrayListMultimap.create());
    
    
    // Dataset-level authorization policy.
    private AuthPolicy authPolicy                         = null;

    /**
     * Record which {@link DataAccessPoint DataAccessPoints} this {@code DataService} is
     * associated with. This is mainly for checking and development.
     * Usually, one {@code DataService} is associated with one {@link DataAccessPoint}.
     */
    private List<DataAccessPoint> dataAccessPoints      = new ArrayList<>(1);

    private volatile DataServiceStatus state            = UNINITIALIZED;

    // DataService-level counters.
    private final CounterSet    counters                = new CounterSet();
    private final AtomicBoolean offlineInProgress       = new AtomicBoolean(false);
    private final AtomicBoolean acceptingRequests       = new AtomicBoolean(true);

    /** Create a {@code DataService} for the given dataset. */
    public DataService(DatasetGraph dataset) {
        this.dataset = dataset;
        counters.add(CounterName.Requests);
        counters.add(CounterName.RequestsGood);
        counters.add(CounterName.RequestsBad);
        // Start ACTIVE. Registration controls visibility.
        goActive();
    }

    /*package*/ void noteDataAccessPoint(DataAccessPoint dap) {
        this.dataAccessPoints.add(dap);
    }

    private String label() {
        StringJoiner sj = new StringJoiner(", ", "[", "]");
        dataAccessPoints.stream()
            .map(DataAccessPoint::getName)
            .filter(x->!x.isEmpty())
            .forEach(sj::add);
        return sj.toString();
    }

    public DatasetGraph getDataset() {
        return dataset;
    }

   public void addEndpointNoName(Operation operation) {
       addEndpointNoName(operation, null);
   }

   public void addEndpointNoName(Operation operation, AuthPolicy authPolicy) {
       addEndpoint(operation, null, authPolicy);
   }

    public void addEndpoint(Operation operation, String endpointName) {
        addEndpoint(operation, endpointName, null);
    }

    public void addEndpoint(Operation operation, String endpointName, AuthPolicy authPolicy) {
        //  Operation -> endpoint.
        Endpoint endpoint = new Endpoint(operation, endpointName, authPolicy);
        addEndpoint(endpoint);
    }

    /** Return the {@linkplain EndpointSet} for the operations for named use. */
    public EndpointSet getEndpointSet(String endpointName) {
        return namedEndpoints.get(endpointName);
    }

    /** Return the {@linkplain EndpointSet} for the operations for unnamed use. */
    public EndpointSet getEndpointSet() {
        return unnamedEndpoints;
    }

    /**
     * Return a collection of all endpoints for this {@linkplain DataService}.
     * This operation is for debug and development - it is not efficient.   
     */
    public Collection<Endpoint> getEndpoints() {
        // Keep separately?
        Set<Endpoint> x = new HashSet<>();
        unnamedEndpoints.forEach((op,ep)->x.add(ep));
        namedEndpoints.forEach((k,eps)->{
            eps.forEach((op,ep)->x.add(ep));
        });
        return x;
    }

    public List<Endpoint> getEndpoints(Operation operation) {
        List<Endpoint> x = operationsMap.get(operation);
        return x;
    }

    /** Return the operations available here.
     *  @see #getEndpoints(Operation) to get the endpoint list
     */
    public Collection<Operation> getOperations() {
        return operationsMap.keySet();
    }

    /** Return the operations available here.
     *  @see #getEndpoints(Operation) to get the endpoint list
     */
    public boolean hasOperation(Operation operation) {
        return operationsMap.keySet().contains(operation);
    }

    public void addEndpoint(Endpoint endpoint) {
        addEndpoint$(endpoint);
    }
        
    private void addEndpoint$(Endpoint endpoint) {
        if ( endpoint.isUnnamed() )
            unnamedEndpoints.put(endpoint);
        else {
            EndpointSet eps = namedEndpoints.computeIfAbsent(endpoint.getName(), (k)->new EndpointSet(k));
            eps.put(endpoint);
        }
        // Cleaner not to have duplicates. But nice to have a (short) list that keeps the create order. 
        if ( ! operationsMap.containsEntry(endpoint.getOperation(), endpoint) )
            operationsMap.put(endpoint.getOperation(), endpoint);
    }
    
    private void removeEndpoint$(Endpoint endpoint) {
        if ( endpoint.isUnnamed() )
            unnamedEndpoints.remove(endpoint);
        else {
            EndpointSet eps = namedEndpoints.get(endpoint.getName());
            if ( eps == null )
                return;
            eps.remove(endpoint);
        }
        operationsMap.remove(endpoint.getOperation(), endpoint);
    }

    //@Override
    public boolean allowUpdate()    { return true; }

    public void goOffline() {
        offlineInProgress.set(true);
        acceptingRequests.set(false);
        state = OFFLINE;
    }

    public void goActive() {
        offlineInProgress.set(false);
        acceptingRequests.set(true);
        state = ACTIVE;
    }

    // Due to concurrency, call isAcceptingRequests().
//    public boolean isActive() {
//        return state != DatasetStatus.ACTIVE;
//    }

    public boolean isAcceptingRequests() {
        return acceptingRequests.get();
    }

    //@Override
    public  CounterSet getCounters() { return counters; }

    //@Override
    public long getRequests() {
        return counters.value(CounterName.Requests);
    }

    //@Override
    public long getRequestsGood() {
        return counters.value(CounterName.RequestsGood);
    }
    //@Override
    public long getRequestsBad() {
        return counters.value(CounterName.RequestsBad);
    }

    /** Counter of active transactions */
    public AtomicLong   activeTxn           = new AtomicLong(0);

    /** Cumulative counter of transactions */
    public AtomicLong   totalTxn            = new AtomicLong(0);

    public void startTxn(TxnType mode) {
        check(DataServiceStatus.ACTIVE);
        activeTxn.getAndIncrement();
        totalTxn.getAndIncrement();
    }

    private void check(DataServiceStatus status) {
        if ( state != status ) {
            String msg = format("DataService %s: Expected=%s, Actual=%s", label(), status, state);
            throw new FusekiException(msg);
        }
    }

    public void finishTxn() {
        activeTxn.decrementAndGet();
    }

    /** Shutdown and never use again. */
    public synchronized void shutdown() {
        if ( state == CLOSING )
            return;
        expel(dataset);
        dataset = null;
        state = CLOSED;
    }

    private void expel(DatasetGraph database) {
        // Text databases.
        // Close the in-JVM objects for Lucene index and databases.
        if ( database instanceof DatasetGraphText) {
            DatasetGraphText dbtext = (DatasetGraphText)database;
            database = dbtext.getBase();
            dbtext.getTextIndex().close();
        }

        boolean isTDB1 = org.apache.jena.tdb.sys.TDBInternal.isTDB1(database);
        boolean isTDB2 = org.apache.jena.tdb2.sys.TDBInternal.isTDB2(database);

        if ( ( isTDB1 || isTDB2 ) ) {
            // JENA-1586: Remove database from the process.
            if ( isTDB1 )
                org.apache.jena.tdb.sys.TDBInternal.expel(database);
            if ( isTDB2 )
                org.apache.jena.tdb2.sys.TDBInternal.expel(database);
        } else
            dataset.close();
    }

    public void setAuthPolicy(AuthPolicy authPolicy) { this.authPolicy = authPolicy; }

    /** Returning null implies no authorization control */
    public AuthPolicy authPolicy() { return authPolicy; }
}

