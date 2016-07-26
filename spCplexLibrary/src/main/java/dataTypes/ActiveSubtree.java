package dataTypes;
 
import java.io.IOException; 
 

import java.util.List;
import java.util.UUID;

import solver.Solver; 
import utilities.UtilityLibrary;
import ilog.concert.IloException;
import ilog.concert.IloLPMatrix;
import ilog.cplex.IloCplex;
import static constantsAndParams.Constants.*;

/**
 * 
 * @author Srini
 * 
 * This class is a wrapper around an ILO-CPLEX subtree being solved
 * 
 * note that this object is NOT SERIALIZABLE
 *
 */

public class ActiveSubtree {
  
    //the CPLEX object representing this partially solved tree 
    private  IloCplex cplex ;
    
    //meta data about the IloCplex object
    private SubtreeMetaData metaData  ;
        
    //a solver object that is used to solve this tree few seconds at a time 
    private Solver solver ;   
   

    //Constructor
    public ActiveSubtree (  NodeAttachment attachment) throws  Exception  {
        
        //initialize the CPLEX object
        cplex= new IloCplex();   
        cplex.importModel(SAV_FILENAME);
        UtilityLibrary.merge(cplex, attachment); 
        
        IloLPMatrix lp = (IloLPMatrix)cplex.LPMatrixIterator().next();        
        metaData = new SubtreeMetaData(   attachment, lp.getNumVars());
        
        //get ourselves a solver
        solver = new Solver( cplex   , metaData);
    }
    
    /**
     * 
     * Solve this subtree for some time, and return nodes which can be migrated.
     * Subtree meta data will be updated by the solver.
     */
    public List<NodeAttachment> solve ( double timeSliceInSeconds, boolean farmingInstruction, double bestKnownGlobalOptimum  ,
            boolean wasCandidateChosenForMigration) throws IloException, IOException{
        
        //solve for some time
        solver.solve( timeSliceInSeconds,     farmingInstruction,   bestKnownGlobalOptimum,   wasCandidateChosenForMigration );
        
        return solver.getMigrationCandidatesList();
    }
 
    public boolean isEntireSubtreeDiscardable() {
        //can we check the cutoff of the ILO-CPLEX object , and use the best known global optimum, before we
        //ask the solver object ?
        return this.solver.isEntireSubtreeDiscardable();
    }
    
    public String toString(){
        String details =this.metaData.getGUID() +NEWLINE;
        details += this.metaData.getRootNodeAttachment().toString();
        return details;
        
    }
    
    public Solution getSolution () throws IloException {
        Solution soln = new Solution () ;
        
        soln.setError(isInError());
        soln.setOptimal(isOptimal());
        soln.setFeasible(isFeasible() );
        soln.setUnbounded(isUnbounded());
        soln.setUnFeasible(isUnFeasible());
        
        soln.setOptimumValue(getObjectiveValue());
        
        if (isOptimalOrFeasible()) UtilityLibrary.addVariablevaluesToSolution(cplex, soln);
        
        return soln;
    }
    
    public boolean isFeasible () throws IloException {
        return cplex.getStatus().equals(IloCplex.Status.Feasible) ;
    }
    
    public boolean isUnFeasible () throws IloException {
        return cplex.getStatus().equals(IloCplex.Status.Infeasible) ;
    }
    
    public boolean isOptimal() throws IloException {
        return cplex.getStatus().equals(IloCplex.Status.Optimal) ;
    }
    public boolean isOptimalOrFeasible() throws IloException {
        return isOptimal()|| isFeasible();
    }
    public boolean isUnbounded() throws IloException {
        return cplex.getStatus().equals(IloCplex.Status.Unbounded) ;
    }
    
    public boolean isInError() throws IloException {
        return cplex.getStatus().equals(IloCplex.Status.Error) ;
    }
  
    public double getObjectiveValue() throws IloException {
        double inferiorObjective = isMaximization?  MINUS_INFINITY:PLUS_INFINITY;
        return isFeasible() || isOptimal() ? cplex.getObjValue():inferiorObjective;
    }
        
    
}
