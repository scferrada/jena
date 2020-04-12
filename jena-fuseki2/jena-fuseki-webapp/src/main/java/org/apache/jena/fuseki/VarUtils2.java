package org.apache.jena.fuseki;

import org.apache.jena.sparql.core.Var;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class VarUtils2 {
    public static List<String> map(Collection<String> varNames, Map<Var, Var> varMap) {
        List<String> result = new ArrayList<>(varNames.size());
        for(String varName : varNames) {
            Var sourceVar = Var.alloc(varName);
            Var targetVar = varMap.get(sourceVar);

            if(targetVar == null) {
                targetVar = sourceVar;
            }

            String targetVarName = targetVar.getVarName();
            result.add(targetVarName);
        }

        return result;
    }
}
