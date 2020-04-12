package org.apache.jena.sparql.engine.join;

import com.eatthepath.jvptree.DistanceFunction;
import com.eatthepath.jvptree.VPTree;
import com.sun.xml.internal.fastinfoset.stax.events.EmptyIterator;
import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.algebra.op.OpKNNSimJoin;
import org.apache.jena.sparql.algebra.op.OpSimJoin;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIterNullIterator;
import org.apache.jena.sparql.serializer.SerializationContext;

import java.util.*;

public class QueryIterVPTSimilarityJoin extends QueryIterSim {

    private List<Binding> rightRows  ;
    private QueryIterator left_;
    private Iterator<Binding> right_;

    private QueryIterVPTSimilarityJoin(QueryIterator left, QueryIterator right, OpSimJoin opSimJoin, ExecutionContext execCxt) {
        super(left,right,execCxt);
        this.left_ = left;
        this.k = ((OpKNNSimJoin)opSimJoin).getK();
        this.attrLeft = opSimJoin.getLeftAttrs();
        this.attrRight = opSimJoin.getRightAttrs();
        this.distVar = opSimJoin.getDist();
        this.distFunc = opSimJoin.getDistanceFunc();
        this.rightRows = Iter.toList(right);
        this.leftRows = new LinkedList<>();
        this.right_ = rightRows.iterator();
        s_countLHS = rightRows.size();
        if (s_countLHS<=k){
            iterator = EmptyIterator.getInstance();
            return;
        }
        s_countResults = s_countLHS * k;
        for(int i=0; i<s_countLHS; i++){
            knn.put(i, new PriorityQueue<>(((OpKNNSimJoin)opSimJoin).getK(), Neighbor.comparator));
        }
        List<VPVector<Binding>> data = materialize();
        vpJoin(data);
        consolidate();
        iterator = results.iterator();
    }

    private void vpJoin(List<VPVector<Binding>> data) {
        DistanceFunction<List<Double>> fun = (l, r) -> {
            double d = 0;
            for(int i=0; i<l.size();i++){
                d += Math.abs(l.get(i)-r.get(i));
            }
            return d;
        };
        VPTree<List<Double>, VPVector<Binding>> index = new VPTree(fun, data);
        int i = 0;
        while (left_.hasNext()) {
            Binding l = left_.nextBinding();
            leftRows.add(l);
            //List<Node> lKey = getKey(l, attrLeft);
            List<Double> lvals = new LinkedList<>();
            for (Var v : attrLeft) {
                lvals.add(Double.parseDouble((String) l.get(v).getLiteralValue()));
            }
            VPVector<Binding> query = new VPVector<>(l, lvals);
            List<VPVector<Binding>> res = index.getNearestNeighbors(query, k+1);
            for(int j=0; j<k+1; j++){
                //List<Node> rKey = getKey(res.get(j).key, attrRight);
                //if(sameKey(rKey,lKey))  continue;
                knn.get(i).add(new Neighbor<>(res.get(j).key, fun.getDistance(query, res.get(j))));
            }
            i++;
        }
    }

    protected List<VPVector<Binding>> materialize() {
        List<VPVector<Binding>> res = new LinkedList<>();
        for(Binding b : rightRows){
            List<Double> row = new LinkedList<>();
            for (Var v : attrRight ) {
                row.add(Double.parseDouble((String) b.get(v).getLiteralValue()));
            }
            res.add(new VPVector<>(b, row));
        }
        return res;
    }

    public static QueryIterator create(QueryIterator left, QueryIterator right, OpSimJoin opSimJoin, ExecutionContext execCxt) {
        if ( ! left.hasNext() ) {
            left.close() ;
            right.close() ;
            return QueryIterNullIterator.create(execCxt) ;
        }
        if ( ! right.hasNext() ) {
            right.close() ;
            return left ;
        }
        return new QueryIterVPTSimilarityJoin(left, right, opSimJoin, execCxt);
    }

    /**
     * Implement this, not hasNext()
     */
    @Override
    protected boolean hasNextBinding() {
        if(slot!=null) return true;
        if(iterator.hasNext()){
            slot = iterator.next();
            return true;
        }
        return false;
    }

    /**
     * Implement this, not next() or nextBinding()
     * Returning null is turned into NoSuchElementException
     * Does not need to call hasNext (can presume it is true)
     */
    @Override
    protected Binding moveToNextBinding() {
        if ( !hasNextBinding() )
            return null;
        Binding x = slot;
        slot = null;
        return x;
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

    static public class VPVector<K> extends LinkedList<Double>{
        private K key;
        private List<Double> content;

        public VPVector(K key, List<Double> content) {
            this.key = key;
            this.content = content;
        }

        public K getKey() {
            return key;
        }

        public List<Double> getContent() {
            return content;
        }

        public Iterator<Double> iterator(){
            return content.iterator();
        }

        public int size(){
            return content.size();
        }

        public Double get(int index){
            return content.get(index);
        }
    }

}
