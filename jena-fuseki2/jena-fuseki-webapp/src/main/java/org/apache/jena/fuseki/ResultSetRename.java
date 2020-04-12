package org.apache.jena.fuseki;

import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingUtils;

import java.util.List;
import java.util.Map;

public class ResultSetRename
        extends ResultSetDecorator
{
    private Map<Var, Var> varMap;
    private List<String> resultVarNames;

    public ResultSetRename(ResultSet decoratee, Map<Var, Var> varMap) {
        this(decoratee, varMap, VarUtils2.map(decoratee.getResultVars(), varMap));
    }

    public ResultSetRename(ResultSet decoratee, Map<Var, Var> varMap, List<String> resultVarNames) {
        super(decoratee);
        this.varMap = varMap;
        this.resultVarNames = resultVarNames;
    }

    @Override
    public QuerySolution next() {
        QuerySolution qs = super.next();
        QuerySolution result = QuerySolutionUtils.rename(qs, varMap);
        return result;
    }

    @Override
    public QuerySolution nextSolution() {
        QuerySolution qs = super.nextSolution();
        QuerySolution result = QuerySolutionUtils.rename(qs, varMap);
        return result;
    }

    @Override
    public Binding nextBinding() {
        Binding binding = super.nextBinding();
        Binding result = BindingUtils2.rename(binding, varMap);
        return result;
    }

    @Override
    public List<String> getResultVars() {
        return resultVarNames;
    }
}