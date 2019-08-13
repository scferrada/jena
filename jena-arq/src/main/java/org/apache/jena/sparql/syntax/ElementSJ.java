package org.apache.jena.sparql.syntax;

import org.apache.jena.sparql.util.NodeIsomorphismMap;

import java.util.ArrayList;
import java.util.List;

public class ElementSJ extends Element {

    List<Element> elements = new ArrayList<>() ;

    public ElementSJ(List<Element> elements) {
        this.elements=elements;
    }

    public ElementSJ() {
        super();
    }

    @Override
    public void visit(ElementVisitor v) {
        v.visit(this);
    }

    @Override
    public int hashCode() {
        int calcHashCode = Element.HashSimjoin ;
        calcHashCode ^=  getElements().hashCode() ;
        return calcHashCode ;
    }

    @Override
    public boolean equalTo(Element el2, NodeIsomorphismMap isoMap) {
        if ( el2 == null ) return false ;

        if ( ! ( el2 instanceof ElementSJ) )
            return false ;
        ElementSJ eu2 = (ElementSJ)el2 ;
        if ( this.getElements().size() != eu2.getElements().size() )
            return false ;
        for ( int i = 0 ; i < this.getElements().size() ; i++ )
        {
            Element e1 = getElements().get(i) ;
            Element e2 = eu2.getElements().get(i) ;
            if ( ! e1.equalTo(e2, isoMap) )
                return false ;
        }
        return true ;
    }

    public void addElement(Element element) {
        elements.add(element);
    }

    public List<Element> getElements(){
        return elements;
    }
}
