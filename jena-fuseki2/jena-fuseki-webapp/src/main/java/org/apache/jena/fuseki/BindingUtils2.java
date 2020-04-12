package org.apache.jena.fuseki;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingHashMap;

import java.util.Iterator;
import java.util.Map;

public class BindingUtils2 {
    public static Binding rename(Binding binding, Map<Var, Var> varMap) {
        BindingHashMap result = new BindingHashMap();

        Iterator<Var> itVars = binding.vars();
        while(itVars.hasNext()) {
            Var sourceVar = itVars.next();

            Node node = binding.get(sourceVar);

            Var targetVar = varMap.get(sourceVar);
            if(targetVar == null) {
                targetVar = sourceVar;
            }

            result.add(targetVar, node);
        }

        return result;
    }
}
