package org.apache.jena.sparql.lang;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryParseException;
import org.apache.jena.query.SimJoinQuery;
import org.apache.jena.query.Syntax;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.lang.sparql_11.ParseException;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParserSPARQLSJ11 extends ParserSPARQL11 {

    @Override
    protected Query parse$(Query query, String queryString) throws QueryParseException {
        Action action = parser -> parser.QueryUnit();

        if (queryString.toLowerCase().contains("similarity join")){
            Query Q1 = new Query();
            Query Q2 = new Query();
            Q1.setSyntax(Syntax.syntaxSPARQL_11);
            Q2.setSyntax(Syntax.syntaxSPARQL_11);

            Pattern pattern = Pattern.compile("(.+) similarity join on ((?:\\((?:\\?[a-zA-Z]\\w* ?)*\\) ?){2}) with distance (\\w+) as (\\?[a-zA-Z]\\w*+) top (\\d+) (.+)");
            Matcher matcher = pattern.matcher(queryString);
            if(!matcher.matches()) throw new QueryParseException("Query does not have a correct Sim. Join syntax", 0,-1);

            String firstSelect = matcher.group(1);
            String attrs = matcher.group(2);
            String distance = matcher.group(3);
            String distVar = matcher.group(4);
            int top = Integer.parseInt(matcher.group(5));
            String secondSelect = matcher.group(6);

            perform(Q1, firstSelect, action);
            perform(Q2, secondSelect, action);

            SimJoinQuery sjQuery = new SimJoinQuery();
            sjQuery.setQ1(Q1);
            sjQuery.setQ2(Q2);
            sjQuery.setDistance(distance);
            sjQuery.setDist(Var.alloc(Var.canonical(distVar)));
            sjQuery.setK(top);

            List<Var> attr1 = new LinkedList<>();
            List<Var> attr2 = new LinkedList<>();
            String[] parts = attrs.split("\\) *\\(");
            String[] str1 = parts[0].replace("(", "").trim().split("\\?");
            String[] str2 = parts[1].replace(")", "").trim().split("\\?");

            for (int i = 1 ; i < str1.length; i++)
                if(str1[i].trim().length()>0)
                    attr1.add(Var.alloc(Var.canonical(str1[i].trim())));
            for (int i = 1 ; i < str2.length; i++)
                if(str2[i].trim().length()>0)
                    attr2.add(Var.alloc(Var.canonical(str2[i].trim())));
            if (attr1.size()!=attr2.size()) throw new IllegalArgumentException("Number of dimensions to join must match.");

            sjQuery.setAttr1(attr1);
            sjQuery.setAttr2(attr2);

            return sjQuery;
        } else {
            query.setSyntax(Syntax.syntaxSPARQL_11);
            perform(query, queryString, action);
            return query;
        }
    }
}

