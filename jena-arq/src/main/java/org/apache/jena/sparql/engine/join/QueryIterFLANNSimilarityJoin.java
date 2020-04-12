package org.apache.jena.sparql.engine.join;

import flann.index.IndexBase;
import flann.index.IndexKDTree;
import flann.metric.Metric;
import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.sparql.algebra.op.OpKNNSimJoin;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIterNullIterator;
import org.apache.jena.sparql.serializer.SerializationContext;

import java.util.*;

public class QueryIterFLANNSimilarityJoin extends QueryIterSim {

    private QueryIterator left_;
    private Iterator<Binding> right_;

    private QueryIterFLANNSimilarityJoin(QueryIterator left, QueryIterator right, OpKNNSimJoin opSimJoin, ExecutionContext execCxt) {
        super(left,right,execCxt);
        this.left_ = left;
        this.k = opSimJoin.getK() + 1;
        this.attrLeft = opSimJoin.getLeftAttrs();
        this.attrRight = opSimJoin.getRightAttrs();
        this.distVar = opSimJoin.getDist();
        this.distFunc = opSimJoin.getDistanceFunc();
        this.rightRows = Iter.toList(right);
        this.leftRows = new LinkedList<>();
        this.right_ = rightRows.iterator();
        s_countLHS = rightRows.size();
        s_countResults = s_countLHS * k;
        for(int i=0; i<s_countLHS; i++){
            knn.put(i, new PriorityQueue<>(opSimJoin.getK(), QueryIterSimilarityJoin.Neighbor.comparator));
        }
        double[][] data = materialize();
        flann(data);
        consolidate();
        iterator = results.iterator();
    }

    private void flann(double[][] data) {
        Metric metric = Distances.getMetric(this.distFunc);
        int[][] indices = new int[1][k];
        double[][] distances = new double[1][k];
        IndexKDTree.BuildParams buildParams = new IndexKDTree.BuildParams(4);
        IndexBase index = new IndexKDTree(metric, data, buildParams);
        index.buildIndex();
        IndexKDTree.SearchParams searchParams2 = new IndexKDTree.SearchParams();
        searchParams2.eps = 0.0f;
        searchParams2.maxNeighbors = k;
        searchParams2.checks = 128;
        int i = 0;
        while (left_.hasNext()) {
            Binding l = left_.nextBinding();
            leftRows.add(l);
            List<Double> lvals = new LinkedList<>();
            for (Var v : attrLeft) {
                lvals.add(((Number)l.get(v).getLiteralValue()).doubleValue());
            }
            double[][] query = new double[1][lvals.size()];
            query[0] = lvals.stream().mapToDouble(Double::doubleValue).toArray();
            index.knnSearch(query, indices, distances, searchParams2);
            for(int j=1; j<k; j++){
                knn.get(i).add(new Neighbor<>(rightRows.get(indices[0][j]), distances[0][j]));
            }
            i++;
        }
    }

    protected double[][] materialize() {
        List<List<Double>> res = new LinkedList<>();
        for(Binding b : rightRows){
            List<Double> row = new LinkedList<>();
            for (Var v : attrRight ) {
                row.add(((Number)b.get(v).getLiteralValue()).doubleValue());
            }
            res.add(row);
        }
        return res.stream().map(l->l.stream().mapToDouble(Double::doubleValue).toArray()).toArray(double[][]::new);
    }

    public static QueryIterator create(QueryIterator left, QueryIterator right, OpKNNSimJoin opSimJoin, ExecutionContext execCxt) {
        if ( ! left.hasNext() ) {
            left.close() ;
            right.close() ;
            return QueryIterNullIterator.create(execCxt) ;
        }
        if ( ! right.hasNext() ) {
            right.close() ;
            return left ;
        }
        return new QueryIterFLANNSimilarityJoin(left, right, opSimJoin, execCxt);
    }

    /**
     * Implement this, not hasNext()
     */
    @Override
    protected boolean hasNextBinding() {
        return iterator.hasNext();
    }

    /**
     * Implement this, not next() or nextBinding()
     * Returning null is turned into NoSuchElementException
     * Does not need to call hasNext (can presume it is true)
     */
    @Override
    protected Binding moveToNextBinding() {
        return iterator.next();
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

    @Override
    public void output(IndentedWriter out, SerializationContext sCxt) {
        super.output(out, sCxt);
    }
}
