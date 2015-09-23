package solvers;

import java.util.ArrayList; 
import java.util.List; 
import org.apache.log4j.Logger;

import solverTypes.NodeAttachment;
import ilog.concert.IloException;
import ilog.concert.IloNumVar;
import ilog.cplex.IloCplex;
import ilog.cplex.IloCplex.BranchDirection;

public class BranchHandler extends IloCplex.BranchCallback{

    //this is the  variable bounds list that is accumulated down the solution tree
    private NodeAttachment nodeAttachment; 

    //add child count for debugging purposes 
    private static int childcount ;
    
    //max kids per node
    private final int MAX_KIDS = 2; 

    //here is the list of new nodes the handler will return, when we farm out nodes
    private  List<NodeAttachment> newNodeList;

    private static Logger logger=Logger.getLogger(BranchHandler.class);

    private long startTime ;
    private int timeSlice  ; //seconds
    
    private boolean isMaximization;
    private double bestKnownOptimum;
    
    static int notFarmWorthy =0;

    static   {
        childcount=0;
    }

    public BranchHandler( NodeAttachment attachment, boolean isMax  ){
        newNodeList = new ArrayList<NodeAttachment>();
        this.nodeAttachment = attachment;		 

        startTime = java.lang.System.currentTimeMillis();
        isMaximization= isMax;
    }	

    public List<NodeAttachment> getNewNodeList () {
        //return read only copy ?
        return newNodeList;
    }	

    public int getChildCount () {
        return childcount;
    }
  
    public void setTimeSlice (int timeSlice) {
        this.timeSlice= timeSlice;
    }
    public void setBestKnownOptimum (double bestKnownOptimum) {
        this.bestKnownOptimum= bestKnownOptimum;
    }

    protected void main() throws IloException  {    
        
        if ( getNbranches()> 0 ){   

            //about to branch   
            
            //increment child count, useful for debug prints
            childcount +=getNbranches() ;
            
            
            //process this node branching
            //if halting, farm the two kids
            //else just accumulate the branching conditions and continue
            
            //first check if this node can be thrown out without farming
            
            if (isFarmWorthy()) {            

                //get the branches about to be created
                IloNumVar[][] vars = new IloNumVar[MAX_KIDS][] ;
                double[ ][] bounds = new double[MAX_KIDS ][];
                BranchDirection[ ][]  dirs = new  BranchDirection[ MAX_KIDS][];
                getBranches(  vars, bounds, dirs);

                if (  getNodeData()==null){
                    //it will be null for the root of every sub problem
                    setNodeData(nodeAttachment);
                };               

                //now get both kids 
                for (int childNum = 0 ;childNum<getNbranches();  childNum++) {                      
                   
                    //apply the bound changes specific to this child
                    NodeAttachment thisChildData = ((NodeAttachment) getNodeData()).createChildNode(
                            dirs[childNum], bounds[childNum], vars[childNum], childNum, getObjValue() );       

                    //prepare to return this node, so we can emit it to disk
                    if (haltingCondition() ) {
                        
                        //collect the child
                        newNodeList.add(thisChildData);
                        
                    } else {
                        //   simply attach node data and continue
                        makeBranch(childNum,thisChildData );
                    }

                }
                
                if (haltingCondition() ) {
                    //prune this node, we have collected its children
                    prune();
                }
                
            } else {
                //prune this node, no point solving it or its children
                prune();
                notFarmWorthy ++;
                logger.debug("number of nodes not FarmWorthy = " + notFarmWorthy); 
                
            } //end if else farm worthy
       
        }   //end if number of branches >0 
     
    }//end method main
        
    //Use a simple halt condition for now
    //should be user configurable
    private boolean haltingCondition (  ) throws IloException {
        /*
        if (HALTING_THRESHOLD <0){
            HALT_FLAG = false;
        } else  if (!HALT_FLAG) {
            //recompute halting condition
            HALT_FLAG= getNnodes64() >= HALTING_THRESHOLD;
        }       
        return HALT_FLAG ;
         */


        logger.info("time elapsed = "+(java.lang.System.currentTimeMillis()-startTime));
        boolean HALT_FLAG =  (timeSlice*1000< (java.lang.System.currentTimeMillis()-startTime));
        if(HALT_FLAG){

            logger.info("time elapsed at halt = "+(java.lang.System.currentTimeMillis()-startTime));
        }
        return HALT_FLAG;
        //return getNnodes64() >= 2;
    }

    private boolean isFarmWorthy(  ) throws IloException{
        return (getObjValue()>bestKnownOptimum && isMaximization) ||
               (getObjValue()<bestKnownOptimum && !isMaximization) ;
    }

}
