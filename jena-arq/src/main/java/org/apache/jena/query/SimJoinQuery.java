package org.apache.jena.query;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpSimJoin;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.lang.ParserSPARQL11;
import org.apache.jena.sparql.lang.ParserSPARQLSJ11;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class SimJoinQuery extends Query{

    protected Query Q1;
    protected Query Q2;
    protected List<Var> leftAttrs;
    protected List<Var> rightAttrs;
    protected DistanceFunction distance;
    protected Var dist;

    private static final Pattern pattern = Pattern.compile("(.+) +similarity +join +on +((?:\\((?:\\?[a-zA-Z]\\w* ?)*\\) *){2}) +with +distance +(\\w+) +as +(\\?[a-zA-Z]\\w*+) +(top|within) +(\\d+\\.?\\d*) +(.+)");

    public static SimJoinQuery parse(String query, ParserSPARQL11.Action action) {
        Query Q1 = new Query();
        Query Q2 = new Query();
        Q1.setSyntax(Syntax.syntaxSPARQL_11);
        Q2.setSyntax(Syntax.syntaxSPARQL_11);

        Matcher matcher = pattern.matcher(query.toLowerCase());
        if(!matcher.matches()) throw new QueryParseException("Query does not have a correct Sim. Join syntax", 0,-1);

        String firstSelect = matcher.group(1);
        String attrs = matcher.group(2);
        String distance = matcher.group(3).toLowerCase().trim();
        String distVar = matcher.group(4);
        String topwithin = matcher.group(5).trim();
        String searchParam  = matcher.group(6);
        String secondSelect = matcher.group(7);

        ParserSPARQLSJ11.perform(Q1, firstSelect, action);
        ParserSPARQLSJ11.perform(Q2, secondSelect, action);

        SimJoinQuery sjQuery;

        if(topwithin.equalsIgnoreCase("top")){
            sjQuery = new KNNSimJoinQuery();
            ((KNNSimJoinQuery)sjQuery).setK(Integer.parseInt(searchParam));
        } else if (topwithin.equalsIgnoreCase("within")){
            sjQuery = new RangeSimJoinQuery();
            ((RangeSimJoinQuery) sjQuery).setRadius(Double.parseDouble(searchParam));
        } else
            throw new QueryParseException("Malformed Similarity Join Query: choose TOP or WITHIN clause",0,0);

        sjQuery.setQ1(Q1);
        sjQuery.setQ2(Q2);
        sjQuery.setDistance(distance);
        sjQuery.setDist(Var.alloc(Var.canonical(distVar)));

        List<Var> attr1 = new LinkedList<>();
        List<Var> attr2 = new LinkedList<>();
        String[] parts = attrs.split("\\) *\\(");
        String[] str1 = parts[0].replace("(", "").trim().split("\\?");
        String[] str2 = parts[1].replace(")", "").trim().split("\\?");

        if (str1.length!=str2.length)
            throw new IllegalArgumentException("Number of dimensions to join must match.");

        for (int i = 1 ; i < str1.length; i++)
            if(str1[i].trim().length()>0)
                attr1.add(Var.alloc(Var.canonical(str1[i].trim())));
        for (int i = 1 ; i < str2.length; i++)
            if(str2[i].trim().length()>0)
                attr2.add(Var.alloc(Var.canonical(str2[i].trim())));

        sjQuery.setLeftAttrs(attr1);
        sjQuery.setRightAttrs(attr2);
        return sjQuery;
    }

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

    public List<Var> getLeftAttrs() {
        return leftAttrs;
    }

    public void setLeftAttrs(List<Var> leftAttrs) {
        this.leftAttrs = leftAttrs;
    }

    public List<Var> getRightAttrs() {
        return rightAttrs;
    }

    public void setRightAttrs(List<Var> rightAttrs) {
        this.rightAttrs = rightAttrs;
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

    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append(Q1.toString())
                .append("SIMILARITY JOIN ON ")
                .append(print(leftAttrs))
                .append(print(rightAttrs))
                .append("\nWITH DISTANCE ")
                .append(distance.toString())
                .append(" AS ")
                .append(dist)
                .append("\nTOP ")
                //.append(k).append("\n")
                .append(Q2.toString());
        return sb.toString();
    }

    private String print(List<Var> attr) {
        StringBuilder sb = new StringBuilder();
        sb.append(" (");
        sb.append(String.join(" ", names(attr)));
        sb.append(") ");
        return sb.toString();
    }

    private Iterable<? extends CharSequence> names(List<Var> attr) {
        List<String> res = new LinkedList<>();
        for (Var v: attr) {
            res.add(v.toString());
        }
        return res;
    }

    public abstract OpSimJoin createOp(Op left, Op right);
}
