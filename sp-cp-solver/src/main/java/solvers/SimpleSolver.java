package solvers;

import java.util.List;

import ilog.concert.IloException;
import ilog.concert.IloLPMatrix;
import ilog.concert.IloNumVar;
import ilog.cplex.IloCplex;



import org.apache.log4j.Logger;

import solverTypes.NodeAttachment;
import solverTypes.Solution;

public class SimpleSolver /*inherits baseSolver ? */{

    //copy pasted CPSOlver for testing purposes

    private static final Logger logger = Logger.getLogger(SimpleSolver.class);

    private  IloCplex cplex ; 
    
    private boolean isMaximization = false;

    //file is the original problem, tree node is the delta that leads to this node
    public SimpleSolver(String filename ){		

        try {
            //setup the problem, start with root node representation
            cplex = new IloCplex();
            cplex.importModel(filename);

            cplex.setParam(IloCplex.Param.MIP.Strategy.Search, IloCplex.MIPSearch.Traditional);	         
            //disableCutsAndHeuristics(); 

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

     * 
     */	
    public Solution solve (  ) throws IloException{

        //start with an empty, invalid solution
        Solution soln = new Solution(isMaximization );	

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

                        logger.debug("Solution Variable " + varName + ": Value = " + varValue);               	

                    }
                }                  	

                logger.info("Solution status = " + cplex.getStatus());
                logger.info("Solution value  = " + cplex.getObjValue());             		
            }     

        }else{
            logger.error("Error: cplex  could not find a feasible solution.");
            soln.setIsError(true); 
        }

        cplex.end();

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

    private void disableCutsAndHeuristics() throws IloException{
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

        cplex.setParam(IloCplex.Param.MIP.Strategy.HeuristicFreq, -1);
    }


}
