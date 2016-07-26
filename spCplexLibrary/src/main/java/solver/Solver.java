package solver;

import java.io.IOException;
import java.util.List;

import callbacks.BranchHandler;
import callbacks.NodeHandler;
import dataTypes.SubtreeMetaData; 
import dataTypes.NodeAttachment;
import dataTypes.Solution;
import ilog.concert.IloException;
import ilog.concert.IloLPMatrix;
import ilog.cplex.IloCplex; 
import static constantsAndParams.Parameters.*;
import static constantsAndParams.Constants.*;

public class Solver {
    
    //this is the CPLEX object we are attached to  
    private IloCplex cplex   ;
    private SubtreeMetaData metaData;
    
    //this is the branch handler for the CPLEX object
    private BranchHandler branchHandler;
    //and the node handler
    private NodeHandler nodeHandler ;
         
    public Solver (IloCplex cplex , SubtreeMetaData metaData ) throws IloException{
            
        this.cplex=cplex;
        this.  metaData=  metaData;
        
        IloLPMatrix lpMatrix = (IloLPMatrix) cplex .LPMatrixIterator().next();
        branchHandler = new BranchHandler(      metaData   );
        nodeHandler = new  NodeHandler (    metaData) ;
        
        this.cplex.use(branchHandler);
        this.cplex.use(nodeHandler);   
        
        setSolverParams();  
    
    }
    
    public void setSolverParams() throws IloException {
        //depth first?
        if ( DEPTH_FIRST_SEARCH) cplex.setParam(IloCplex.Param.MIP.Strategy.NodeSelect, ZERO); 
        
        //MIP gap
        if ( RELATIVE_MIP_GAP>ZERO) cplex.setParam( IloCplex.Param.MIP.Tolerances.MIPGap, RELATIVE_MIP_GAP);

        //others
    }
    
    public boolean isEntireSubtreeDiscardable() {
        return this.branchHandler.isEntireSubtreeDiscardable();
    }
    
    public List<NodeAttachment> getMigrationCandidatesList(){
        return this.branchHandler.getMigrationCandidatesList();
    }

    /**
     * 
     if this subtree had offered a node for migration, whether or not it was actually migrated is specified by wasCandidateChosenForMigration  
     */
    public IloCplex.Status solve(double timeSliceInSeconds,   boolean farmingInstruction, double bestKnownGlobalOptimum, boolean wasCandidateChosenForMigration   ) 
            throws IloException, IOException{
        
        //inform branch handler of farming instruction and current incumbent, and whether any farmed node was  Chosen For Migration
        branchHandler.reset( farmingInstruction,   wasCandidateChosenForMigration , bestKnownGlobalOptimum   );
       
        cplex.setParam(IloCplex.Param.TimeLimit, timeSliceInSeconds); 
        cplex.solve();
        
        return cplex.getStatus();
    }
    
   
    
}
