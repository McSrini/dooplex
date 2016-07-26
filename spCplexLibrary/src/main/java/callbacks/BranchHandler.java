package callbacks;
 
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import utilities.UtilityLibrary;
import static constantsAndParams.Constants.*;
import static constantsAndParams.Parameters.*; 
import dataTypes.NodeAttachment;
import dataTypes.SubtreeMetaData;
import ilog.concert.IloException;
import ilog.concert.IloNumVar;
import ilog.cplex.IloCplex;
import ilog.cplex.IloCplex.BranchDirection;
import ilog.cplex.IloCplex.NodeId;

/**
 * 
 * @author srini
 * 
 * 1) accumulates branching conditions and any other variable bounds , into the kids
 * 2) farms out node when the ILOCPLEX object is too big, or when instructed to farm
 * 3) discards nodes, or entire subtree,  which are inferior to already known incumbent
 * 4) implements distributed MIP gap by using the bestKnownGlobalOptimum
 *
 */
public class BranchHandler extends IloCplex.BranchCallback{
    
    //whether we are told to farm  
    private   boolean farmingInstruction;
    //depending on how the tree grows, the handler may overrule the farming Instruction
    private boolean farmingDecision;
     
    //a node which could be migrated away
    private  List<NodeAttachment>  migrationCandidatesList ; //list will have <= 1 item
    //Driver makes a decision whether to actually migrate this node or not
    private boolean wasCandidateChosenForMigration = false;
    private boolean candidateOfferedForMigration = false;
    
    //best known optimum is used to prune nodes
    private double bestKnownGlobalOptimum;
    

    //meta data of the subtree which we are monitoring
    private SubtreeMetaData metaData;
    IloNumVar[]  modelIntVars ;
    
    public BranchHandler (SubtreeMetaData metaData) {
        this.  metaData= metaData;
        modelIntVars = metaData.getIntvars();
        migrationCandidatesList= new ArrayList<NodeAttachment>();
    }
 
    
    public void reset( boolean farmingInstruction, boolean wasCandidateChosenForMigration , 
            double bestKnownGlobalOptimum) {
        this.bestKnownGlobalOptimum=bestKnownGlobalOptimum;
        this.wasCandidateChosenForMigration=wasCandidateChosenForMigration;
        this.farmingInstruction=farmingInstruction;
        migrationCandidatesList= new ArrayList<NodeAttachment>();
    } 
    
    public List<NodeAttachment> getMigrationCandidatesList(){
        return migrationCandidatesList;
    }
    
    public boolean isEntireSubtreeDiscardable() {
        return this.metaData.getIsEntireTreeDiscardable();
    }
    
    protected void main() throws IloException {

        //is further processing really required for this node?
        boolean isProcessingRequired  = true;
        
        if ( getNbranches()> ZERO ){  
          
            //tree is branching
            
            //first check if entire tree can be discarded
            if (canTreeBeDiscarded()    ){
                
                //no point solving this tree any longer 
                metaData.setEntireTreeDiscardable();
                isProcessingRequired= false;
                abort();
                
            }else {
                
                //check if this node can be discarded
                if ( canNodeBeDiscarded()) {
                    //prune this node
                    if (isSubtreeRoot()){
                        //no point solving this tree any longer 
                        metaData.setEntireTreeDiscardable();
                        abort();
                                                
                    } else {
                        //only this node is useless
                        prune();                        
                    }
                    
                    isProcessingRequired = false;
                }
                
            }
            
            //at this point we know if processing is required for this node
            
            if (isProcessingRequired) {
                
                //check to see if we had offered up a node and it was chosen for migration
                boolean mustPrune =  wasCandidateChosenForMigration && this.candidateOfferedForMigration;
                
                //note that, if we had not offered up a node for migration, user can supply any value for wasCandidateChosenForMigration
                                 
                if (mustPrune) {
                    //prune this node, it has been moved and will be solved somewhere else
                    prune();
                    //we are not offering anything for migration right now
                    candidateOfferedForMigration=false;
                    
                } else {
                    
                    //do not prune this node, continue branching
                    
                    //get the node attachment for this node, any child nodes will accumulate the branching conditions
                    NodeAttachment nodeData = (NodeAttachment) getNodeData();
                    if (nodeData==null ) { //it will be null for subtree root
                        NodeAttachment subTreeRoot = metaData.getRootNodeAttachment();
                        nodeData=new NodeAttachment (   subTreeRoot.isEasy(),  
                                subTreeRoot.getUpperBounds(), 
                                subTreeRoot.getLowerBounds(),  
                                subTreeRoot.getDepthFromOriginalRoot(), 
                                ZERO);         
                    }
                    //update the node attachment with end time                
                    if (nodeData.getEndTimeFor_LP_Relaxation()<=ZERO)  {
                        nodeData.setEndTimeFor_LP_Relaxation(System.currentTimeMillis());
                        setNodeData(nodeData);
                    }
                    
                    
                    //make the farming decision. 
                    //If this node is farmed and migrated , then its children will not spawn here
                    makeFarmingDecison(nodeData);
                    
                    //if decision is to farm, we can add this node to migration candidate list, and abort()
                    //otherwise we must let its kids spawn
                    if (farmingDecision) {
                        
                        this.migrationCandidatesList.add( nodeData );     
                        this.candidateOfferedForMigration=true;
                        abort();
                        
                        //there is a chance here that the same node will repeatedly be offered as a migration candidate, and
                        //repeatedly not chosen for migration. This can be avoided by migrating candidates from large trees.
                        
                    }else {
                        
                        this.candidateOfferedForMigration=false;
                        
                        //get the branches about to be created
                        IloNumVar[][] vars = new IloNumVar[TWO][] ;
                        double[ ][] bounds = new double[TWO ][];
                        BranchDirection[ ][]  dirs = new  BranchDirection[ TWO][];
                        getBranches(  vars, bounds, dirs);
                        
                        //get bound tightenings
                        Map< IloNumVar,Double > upperBoundTightenings = findIntegerBounds(true);
                        Map< IloNumVar,Double > lowerBoundTightenings = findIntegerBounds(false);
                        
                        //allow  both kids to spawn
                        for (int childNum = ZERO ;childNum<getNbranches();  childNum++) {    
                            //apply the bound changes specific to this child
                            NodeAttachment thisChild  = UtilityLibrary.createChildNode( nodeData,
                                    dirs[childNum], bounds[childNum], vars[childNum]  , isChildEasy() ); 
                            
                            //apply bound tightenings
                            
                            for (Entry<IloNumVar, Double> entry : upperBoundTightenings.entrySet()){
                                UtilityLibrary. mergeBound(thisChild, entry.getKey().getName(), entry.getValue()  , true);
                            }
                            for (Entry<IloNumVar, Double> entry : lowerBoundTightenings.entrySet()){
                                UtilityLibrary. mergeBound(thisChild, entry.getKey().getName(), entry.getValue()  , false);
                            }

                            //   create the  kid,  and attach node data  to the kid
                            NodeId nodeID = makeBranch(childNum,thisChild );
                            //make a note of the new child in the meta data
                            this.metaData.addUnsolvedLeafNodes(nodeID, thisChild);
                            
                        }
                        
                    }//end if farming decision
                    
                } //end if else wasCandidateChosenForMigration              
                
            } //end if isProcessingRequired
            
        }//end if getNbranches >0
        
    }//end main
    
    private boolean canNodeBeDiscarded () throws IloException {
        boolean result = false;

        //get LP relax value
        double nodeObjValue = getObjValue();
        
        result = isMaximization  ? 
                    (nodeObjValue < getCutoff()) || (nodeObjValue <= bestKnownGlobalOptimum )  : 
                    (nodeObjValue > getCutoff()) || (nodeObjValue >= bestKnownGlobalOptimum );

        return result;
    }
    
    //if haltingCondition, then this ILOCLPEX object can be discarded
    private boolean canTreeBeDiscarded(  ) throws IloException{        
        
        double metric =  getBestObjValue() -bestKnownGlobalOptimum ;
        metric = metric /(EPSILON +bestKnownGlobalOptimum);
        
        //|bestnode-bestinteger|/(1e-10+|bestinteger|) 
        boolean mipHaltCondition =  RELATIVE_MIP_GAP >= Math.abs(metric)  ;
        
        //also halt if we cannot do better than bestKnownGlobalOptimum
        boolean inferiorityHaltConditionMax = isMaximization && 
                                              (bestKnownGlobalOptimum>=getIncumbentObjValue()) && 
                                              (bestKnownGlobalOptimum>=getBestObjValue());
        boolean inferiorityHaltConditionMin = !isMaximization && 
                                               (bestKnownGlobalOptimum<=getIncumbentObjValue()) && 
                                               (bestKnownGlobalOptimum<=getBestObjValue());
         
        
        return  mipHaltCondition || inferiorityHaltConditionMin|| inferiorityHaltConditionMax;       
      
    }
    
    private boolean isSubtreeRoot () throws IloException {
        
        boolean isRoot = true;
        
        if (getNodeData()!=null  ) {
            NodeAttachment thisNodeData =(NodeAttachment) getNodeData();
            if (thisNodeData.getDepthFromSubtreeRoot()>ZERO) {
                
                isRoot = false;
                
            }
        }    
        
        return isRoot;
        
    }
    
    //the branch handler makes the farming decision    
    private void makeFarmingDecison ( NodeAttachment thisNodeData) throws IloException{
        
        //default is to obey the instruction
        farmingDecision= farmingInstruction   ; 
        
        long activeNodeCount = getNremainingNodes64();
        
        if (isSubtreeRoot() || thisNodeData.isEasy()) {
            //do not farm the root node of any subtree
            //only hard nodes are potentially farmed
            farmingDecision = false;             
        }  else  if ( activeNodeCount >= MAX_LEAFS_PER_SUBTREE) {
            //farm if the tree has grown too big, and this node does not take too long to solve
            farmingDecision = thisNodeData.getTimeFor_LP_Relaxation()<LP_RELAX_THRESHOLD_FOR_FARMING_MILLISEC; 
             
        }
               
    }
    
    private boolean isChildEasy(){
        //fill up later
        return false;
    }
    
    private Map< IloNumVar,Double > findIntegerBounds (boolean isUpperBound) throws IloException {
        Map< IloNumVar,Double > results = new  HashMap< IloNumVar,Double >();
        
        double[] values  = isUpperBound ? getUBs( modelIntVars): getLBs(modelIntVars);

        for (int index = ZERO ; index <modelIntVars.length; index ++ ){

            results.put( modelIntVars[index] , values[index]) ;
        }
        return results;
    }
    
}
