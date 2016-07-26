package dataTypes;

import static constantsAndParams.Constants.*;
import ilog.concert.IloNumVar;
import ilog.cplex.IloCplex.NodeId;

import java.util.*;

/**
 * 
 * @author srini
 * 
 * This object holds all the meta data associated with the ActiveSubtree.
 * Since we cannot traverse the ILOCPLEX object at will, we store relevant information here.
 *
 */
public class SubtreeMetaData {

    //GUID used to identify the ActiveSubtree
    private final String guid ;
  
    //keeps note of all the INT variables in the model
    //used  to find bound tightenings when spawning kids.
    private final IloNumVar[] intVars ;  
    
    //keep note of the root Node Attachment used to create this subtree
    private final NodeAttachment rootNodeAttachment ;
    
    //sometimes we find that the entire subtree can be discarded
    private boolean canDiscardEntireSubTree  = false;  
        
    //keep a list of unsolved leaf nodes.
    //These may be useful later on, when making farming decisions.
    //These are child nodes that were spawned, but never picked up for solving.
    private Map<NodeId, NodeAttachment> unsolvedLeafNodes = new HashMap<NodeId, NodeAttachment>();
    
    public SubtreeMetaData( NodeAttachment attachment, IloNumVar[] intVars){
        guid = UUID.randomUUID().toString();
        rootNodeAttachment=attachment;
        this.intVars= intVars;
    }
    
    public String getGUID(){
        return this.guid;
    }
    
    public IloNumVar[]   getIntvars (){
        return intVars;
    }
    
    public NodeAttachment getRootNodeAttachment(){
        return rootNodeAttachment;
    }
    
    public void addUnsolvedLeafNodes (NodeId nodeID, NodeAttachment attachment) {
        unsolvedLeafNodes.put(nodeID, attachment);
    }
    
    public void removeUnsolvedLeafNodes (NodeId nodeID) {        
        unsolvedLeafNodes.remove(nodeID);
    }
    
    public Map<NodeId, NodeAttachment> getUnsolvedLeafNodes () {
        return Collections.unmodifiableMap(unsolvedLeafNodes);
    }
    
    public void setEntireTreeDiscardable() {
        this.canDiscardEntireSubTree= true;
    }
    
    public boolean getIsEntireTreeDiscardable() {
        return this.canDiscardEntireSubTree ;
    }
    
}
