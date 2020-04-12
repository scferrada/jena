package org.apache.jena.sparql.engine.join;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.graph.Node;
import org.apache.jena.query.DistanceFunction;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.op.OpSimJoin;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIter2;

import java.util.*;

public abstract class QueryIterSim extends QueryIter2 {

    protected int k = 1;
    protected Iterator<Binding> iterator = null;
    protected long s_countLHS = 0;
    protected long s_countRHS = 0;
    protected long s_countResults = 0;

    protected Var distVar = null;
    protected DistanceFunction distFunc = null;

    protected List<Binding> leftRows = null;
    protected Iterator<Binding> left;
    protected QueryIterator right;

    protected List<Var> attrRight;
    protected List<Var> attrLeft;

    protected Binding slot = null;
    protected boolean finished = false;

    protected List<Binding> rightRows  ;

    protected Map<Integer, PriorityQueue<Neighbor>> knn = new HashMap<>();
    protected List<Binding> results = new LinkedList<>();
    protected Map<String, Pair<Double, Double>> norm = null;

    public QueryIterSim(QueryIterator left, QueryIterator right, ExecutionContext execCxt) {
        super(left, right, execCxt);
    }

    public QueryIterSim(QueryIterator left, QueryIterator right, int k, DistanceFunction distFunc, Var v, List<Var> attrs, Map<String, Pair<Double, Double>> minmax, ExecutionContext execCxt) {
        this(left, right, k, distFunc, v, attrs, execCxt);
        this.norm = minmax;
    }

    public QueryIterSim(QueryIterator left, QueryIterator right, int k, DistanceFunction distFunc, Var v, List<Var> attrs, ExecutionContext execCxt) {
        super(left, right, execCxt);
    }

    public static QueryIterator create(QueryIterator left, QueryIterator right, OpSimJoin opSimJoin, ExecutionContext execCxt) {
        return opSimJoin.createIterator(left, right, execCxt);
    }

    public static QueryIterator createNL(QueryIterator left, QueryIterator right, int k, DistanceFunction distFunc, Var v, List<Var> attrs, Map<String, Pair<Double, Double>> minmax, ExecutionContext execCxt) {
        return new QueryIterSimilarityJoin(left, right, k, distFunc, v, attrs, minmax,execCxt);
    }

    protected void consolidate() {
        for (int i: knn.keySet()) {
            List<Binding> res = Algebra.join(leftRows.get(i), knn.get(i), distVar);
            results.addAll(res);
        }
    }

    protected static boolean sameKey(List<Node> rKey, List<Node> lKey) {
        if(rKey.size()!=lKey.size())
            return false;
        for(int i=0; i<rKey.size();i++){
            if(!rKey.get(i).getURI().equals(lKey.get(i).getURI()))
                return false;
        }
        return true;
    }

    protected List<Node> getKey(Binding binding, List<Var> vars) {
        List<Node> key = new ArrayList<>();
        for (Iterator<Var> it = binding.vars(); it.hasNext(); ) {
            Var v = it.next();
            if(!vars.contains(v)){
                key.add(binding.get(v));
            }
        }
        return key;
    }

    @Override
    public void output(IndentedWriter w){
        while(iterator.hasNext()) {
            Binding b = iterator.next();
//            String txt = String.format("<%s> <%s> %s",
//            b.get(new Var("id1")).getURI(),
//            b.get(new Var("id2")).getURI(),
//            b.get(new Var("d")).getLiteralValue());
            w.write(b.toString() + "\n");
        }
        w.close();
    }

    public static class Neighbor<K>{
        private K key;
        private double distance;

        static public Comparator<Neighbor> comparator= (n1, n2) -> n1.distance>n2.distance? -1:1;

        protected Neighbor(K key, double distance) {
            this.key = key;
            this.distance = distance;
        }

        public K getKey() {
            return key;
        }

        public double getDistance() {
            return distance;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Neighbor<?> neighbor = (Neighbor<?>) o;
            return key.equals(neighbor.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, distance);
        }
    }
}
