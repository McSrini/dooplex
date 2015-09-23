package solverTypes;

import ilog.concert.IloNumVar;
import ilog.cplex.IloCplex.BranchDirection;

import java.util.Collections;
import java.util.Hashtable; 
import java.util.Map;
import java.util.Map.Entry;

import com.google.gson.Gson;

//this is the data accumulated in every tree node that is created by a CPLEX branch
//
//depth is the distance from the root node  ( i.e. the distance from the original problem)
//depth is needed to make decisions about BFS  DFS 
//
//when writing a node to disk, this is the data structure we need to
//export and then import back to reconstruct the CPLEX node representing an interior tree node
//

public class NodeAttachment {

    // distance From Original Node
    private int depth;   

    //for debugging purposes, these two are useful
    //we are not using the CPLEX generated ID
    //private String parentNodeID;
    //private String thisNodeID;

    //every time there is a branching on a variable, we update on of these lists with the
    //new bound corresponding to the branching condition
    private Map< String, Double > upperBounds ;
    private Map< String, Double > lowerBounds ;
    
    private double parentsLPRelaxOptValue;
    
    public double getParentLPRElaxOptimumValue(){
        return parentsLPRelaxOptValue ;
    }

    public void setParentLPRElaxOptimumValue(double val){
        parentsLPRelaxOptValue=val ;
    }

    public NodeAttachment (int depth, double parentsLPRelaxOptValue /*, String myID, String pid*/) {

        upperBounds = new Hashtable<   String, Double>();
        lowerBounds = new Hashtable<   String, Double>();

        this.depth = depth;
        this.parentsLPRelaxOptValue = parentsLPRelaxOptValue;

        //thisNodeID=myID;
        //parentNodeID=pid;

    }

    public static NodeAttachment fromJSONString(String json){
        return (new Gson()).fromJson(json.trim(), NodeAttachment.class);
    }

    public String toJSONString(){
        return(new Gson()).toJson(this);
    }



    public int getDepth( ) {
        return depth ;
    }

    /*
	public String getMyId( ) {
		return thisNodeID ;
	}

	public String getParentID( ) {
		return parentNodeID ;
	}	
     */

    public Map< String, Double >   getUpperBounds   () {
        return Collections.unmodifiableMap(upperBounds);
    }

    public Map< String, Double >   getLowerBounds   () {
        return Collections.unmodifiableMap(lowerBounds);
    }

    //clone this object , and apply new bounds
    //use this method to create node data for a child node
    public NodeAttachment createChildNode (BranchDirection[ ] directionArray, 
            double[ ] boundArray, IloNumVar[] varArray, int childNum, double parentsLPRelaxObjValue) {

        //depth of child is 1 more than parent
        NodeAttachment child = new NodeAttachment(depth +1 ,parentsLPRelaxObjValue );

        //copy parents bounds
        for (Entry <String, Double> entry : upperBounds.entrySet()){
            child.mergeBound(entry.getKey(), entry.getValue(), true);
        }
        for (Entry <String, Double> entry : lowerBounds.entrySet()){
            child.mergeBound(entry.getKey(), entry.getValue(), false);
        }

        //now apply the new bounds to the existing bounds
        for (int index = 0 ; index < varArray.length; index ++) {							
            child.mergeBound(varArray[index].getName(), boundArray[index] , 
                    directionArray[index].equals(BranchDirection.Down));
        }

        return child;
    }

    //merge this bound into existing bounds, return true if added or merged, false if no effect
    private boolean mergeBound(String varName, double value, boolean isUpperBound) {
        boolean isMerged = false;

        //the following logic is written in a verbose manner for easy understanding
        if (isUpperBound){
            if (upperBounds.containsKey(varName)) {
                if (value < upperBounds.get(varName)){
                    //update the more restrictive upper bound
                    upperBounds.put(varName, value);
                    isMerged = true;
                }
            }else {
                //add the bound
                upperBounds.put(varName, value);
                isMerged = true;
            }
        } else {
            if (lowerBounds.containsKey(varName)) {
                if (value > lowerBounds.get(varName)){
                    //update the more restrictive lower bound
                    lowerBounds.put(varName, value);
                    isMerged = true;
                }				
            }else {
                //add the bound
                lowerBounds.put(varName, value);
                isMerged = true;
            }
        }

        return isMerged;
    }

}
