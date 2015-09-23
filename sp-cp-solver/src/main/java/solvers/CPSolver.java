package solvers;

import ilog.concert.IloException;
import ilog.concert.IloLPMatrix;
import ilog.concert.IloNumVar; 
import ilog.cplex.IloCplex;

import java.io.IOException;
import java.util.List;
import java.util.Map;  
import org.apache.log4j.Logger;

import solverTypes.NodeAttachment; 
import solverTypes.Solution;

public class CPSolver {

    private static final Logger logger = Logger.getLogger(CPSolver.class);

    private  IloCplex cplex ;
    private  BranchHandler branchHandler;
    private boolean isMaximization = true; 

    //file is the original problem, attachment node is the
    // delta from the original that leads to this node
    public CPSolver(String filename, NodeAttachment node , boolean isMax ){	
        
        isMaximization= isMax;

        try {
            //setup the problem, start with root node representation
            cplex = new IloCplex();						
            cplex.importModel(filename);

            cplex.setParam(IloCplex.Param.MIP.Strategy.Search, IloCplex.MIPSearch.Traditional);
            
            //do a depth first search
            cplex.setParam(IloCplex.Param.MIP.Strategy.NodeSelect, 0);	 
            //logger.debug("depth first search in effect");

            //disableCuts();
            //disableHeuristics();

            //check if this is the original problem node or interior node
            if (node !=null){
                //interior node
                //apply branch conditions to arrive at representation of interior node
                convertToInteriorNode(node );               
            } else {
                //original problem, start with an empty attachment                
                node  = new NodeAttachment(0, isMaximization?  Solution.DOUBLE_MAX: Solution.DOUBLE_MIN);                  
            }

            //setup the handler	
            branchHandler= new BranchHandler(   node , isMaximization );
            cplex.use(branchHandler);  

        } catch (IloException ex) {			
            logger.error(ex); 
        }         
    }

    /**
     * 
     * @param nodeToSolve
     * @param newNodeList
     * @return
     * 
     * solve nodeToSolve until halt-condition, and return Solution
     * Also append to newNodeList, which are the new child and grand-child nodes generated 
     * 
     * Tachyon (shared memory) can be used later for implementing a better halting condition
     * @throws IloException 
     * @throws IOException 
     * 
     */	
    public Solution solve ( List<NodeAttachment> newNodeList, int timeSliceInSeconds, double bestKnownOptimum  ) 
            throws IloException{

        //define an empty, invalid solution variable
        Solution soln = new Solution(isMaximization );	
        
        //inform the branch handler of the best known optimum and time slice
        //these are used to determine the halting condition and pruning policy
        branchHandler.setTimeSlice( timeSliceInSeconds);
        branchHandler.setBestKnownOptimum( bestKnownOptimum);

        if ( cplex.solve() ) {

            boolean isErroneus = cplex.getStatus().equals(IloCplex.Status.Error);
            if (!isErroneus) {

                //construct the solution object, so that we can return it to caller

                soln.setIsError(isErroneus);
                soln.setIsUnbounded(cplex.getStatus().equals(IloCplex.Status.Unbounded));
                soln.setIsFeasible( cplex.getStatus().equals(IloCplex.Status.Feasible));
                soln.setIsOptimal( cplex.getStatus().equals(IloCplex.Status.Optimal));

                if (soln.getIsOptimal()) {
                    soln.setOptimumValue( cplex.getObjValue()); 

                    IloLPMatrix lpMatrix = (IloLPMatrix)cplex.LPMatrixIterator().next();

                    //WARNING: we assume that every variable appears in at least 1 constraint or variable bound
                    //Otherwise, this method of getting all the variables from the matrix may not yield all the
                    //variables
                    IloNumVar[] variables = lpMatrix.getNumVars();
                    double[] variableValues = cplex.getValues(variables);                 

                    for ( int index = 0; index < variableValues.length; index ++){

                        String varName = variables[index].getName();
                        double varValue = variableValues[index];
                        soln.setVariableValue (varName,  varValue);

                    }
                }                  	         		
            } else {
                logger.error("Error: cplex  error.");
                soln.setIsError(true);    
                //should we abort in case of error?
            }

        }else{
            logger.error("Error: cplex  could not find a feasible solution.");
            soln.setIsError(true); 
            //should we abort in this case?
        }

        cplex.end();

        //append the new nodes (i.e. the farmed out nodes) to the existing node list
        if (newNodeList!=null){        	        	 
            newNodeList.addAll(this.branchHandler.getNewNodeList());
        }       

        return soln;
    }

    public static String getVersion(){
        try {
            return (new IloCplex()).getVersion();
        } catch (IloException e) {
            // TODO Auto-generated catch block
            //e.printStackTrace();
            return "-1";
        }
    }

    private void disableCuts () throws IloException{

        cplex.setParam(IloCplex.Param.MIP.Cuts.Cliques, -1);
        cplex.setParam(IloCplex.Param.MIP.Cuts.Covers, -1);
        cplex.setParam(IloCplex.Param.MIP.Cuts.Disjunctive, -1);
        cplex.setParam(IloCplex.Param.MIP.Cuts.FlowCovers, -1);
        cplex.setParam(IloCplex.Param.MIP.Cuts.Gomory, -1);
        cplex.setParam(IloCplex.Param.MIP.Cuts.GUBCovers, -1);
        cplex.setParam(IloCplex.Param.MIP.Cuts.Implied, -1);
        cplex.setParam(IloCplex.Param.MIP.Cuts.LiftProj, -1);
        cplex.setParam(IloCplex.Param.MIP.Cuts.LocalImplied, -1);
        cplex.setParam(IloCplex.Param.MIP.Cuts.MCFCut, -1);
        cplex.setParam(IloCplex.Param.MIP.Cuts.MIRCut, -1);
        cplex.setParam(IloCplex.Param.MIP.Cuts.PathCut, -1);
        cplex.setParam(IloCplex.Param.MIP.Cuts.ZeroHalfCut, -1);             

    }

    private void disableHeuristics() throws IloException{

        cplex.setParam(IloCplex.Param.MIP.Strategy.HeuristicFreq, -1);
    }

    //use the CPLEX object imported (i.e. the root node) and apply all
    //the bounds to arrive at the interior node	
    private void  convertToInteriorNode( NodeAttachment node   ) throws IloException {

        IloLPMatrix lpMatrix = (IloLPMatrix)cplex.LPMatrixIterator().next();

        //WARNING : we assume that every variable appears in at least 1 constraint or variable bound
        IloNumVar[] variables = lpMatrix.getNumVars();

        for (int index = 0 ; index <variables.length; index ++ ){

            IloNumVar thisVar = variables[index];
            updateVariableBounds(thisVar,node.getLowerBounds(), false );
            updateVariableBounds(thisVar,node.getUpperBounds(), true );

        }       
    }

    //find 'thisVar' , and update variable bounds
    private static void updateVariableBounds(IloNumVar thisVar, Map< String, Double > bounds, boolean isUpperBound   ) 
            throws IloException{

        String varName = thisVar.getName();
        boolean isPresentInNewBounds = bounds.containsKey(varName);

        if (isPresentInNewBounds) {
            double newBound =   bounds.get(varName)  ;
            if (isUpperBound){
                if ( thisVar.getUB() > newBound ){
                    //update the more restrictive upper bound
                    thisVar.setUB( newBound );
                }
            }else{
                if ( thisVar.getLB() < newBound){
                    //update the more restrictive lower bound
                    thisVar.setLB(newBound);
                }
            }				
        }

    }	 

}
