package org.apache.jena.sparql.engine.join;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.sparql.algebra.op.OpSimJoin;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIter2;

public class QueryIterExternalQuickjoin extends QueryIter2 {


    public QueryIterExternalQuickjoin(QueryIterator left, QueryIterator right, OpSimJoin sim, ExecutionContext execCxt) {
        super(left, right, execCxt);
        Model m = ModelFactory.createDefaultModel();
        //sim.getQuery().getQueryPattern();
        //m.add(null);
        //Dataset d = org.apache.jena.tdb.TDBFactory.createDataset();

    }
    /**
     * Cancellation of the query execution is happening
     */
    @Override
    protected void requestSubCancel() {

    }

    /**
     * Pass on the close method - no need to close the left or right QueryIterators passed to the QueryIter2 constructor
     */
    @Override
    protected void closeSubIterator() {

    }

    /**
     * Implement this, not hasNext()
     */
    @Override
    protected boolean hasNextBinding() {
        return false;
    }

    /**
     * Implement this, not next() or nextBinding()
     * Returning null is turned into NoSuchElementException
     * Does not need to call hasNext (can presume it is true)
     */
    @Override
    protected Binding moveToNextBinding() {
        return null;
    }
}
