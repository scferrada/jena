package org.apache.jena.query;

import org.apache.jena.sparql.core.Var;

import java.util.List;

public class SimJoinQuery extends Query{

    private Query Q1;
    private Query Q2;
    private List<Var> attr1;
    private List<Var> attr2;
    private DistanceFunction distance;
    private Var dist;
    private int k;

    public Query getQ1() {
        return Q1;
    }

    public void setQ1(Query q1) {
        Q1 = q1;
    }

    public Query getQ2() {
        return Q2;
    }

    public void setQ2(Query q2) {
        Q2 = q2;
    }

    public List<Var> getAttr1() {
        return attr1;
    }

    public void setAttr1(List<Var> attr1) {
        this.attr1 = attr1;
    }

    public List<Var> getAttr2() {
        return attr2;
    }

    public void setAttr2(List<Var> attr2) {
        this.attr2 = attr2;
    }

    public DistanceFunction getDistance() {
        return distance;
    }

    public void setDistance(String distance) {
        this.distance = DistanceFunction.valueOf(distance.toUpperCase());
    }

    public Var getDist() {
        return dist;
    }

    public void setDist(Var dist) {
        this.dist = dist;
    }

    public int getK() {
        return k;
    }

    public void setK(int k) {
        this.k = k;
    }

    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append(Q1.toString())
                .append("SIMILARITY JOIN ON ")
                .append(print(attr1))
                .append(print(attr2))
                .append("\nWITH DISTANCE ")
                .append(distance.toString())
                .append(" AS ")
                .append(dist)
                .append("\nTOP ")
                .append(k).append("\n")
                .append(Q2.toString());
        return sb.toString();
    }

    private String print(List<Var> attr) {
        StringBuilder sb = new StringBuilder();
        sb.append(" (");
        for (Var v: attr) {
            sb.append(v.toString())
                    .append(" ");
        }
        sb.append(") ");
        return sb.toString();
    }
}
