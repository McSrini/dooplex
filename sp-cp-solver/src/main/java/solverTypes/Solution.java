package solverTypes;

import java.util.Collections;
import java.util.Hashtable;
import java.util.Map;

import com.google.gson.Gson;

public class Solution {
    
    public static final double DOUBLE_MAX = 1000000000;
    public static final double DOUBLE_MIN = -1*DOUBLE_MAX;

    private double optimumValue;	

    private boolean isFeasible;	
    private boolean isOptimal;
    private boolean isUnbounded; 
    private boolean isError; //error has occurred

    //a map of variable names, and their values
    private  Map< String, Double> variableMap ;

    public Solution(boolean isMax) {
        //empty solution is infeasible, so it will be inferior to 
        //any other feasible or optimal solution
        isFeasible = false;
        isOptimal=false;
        isError=false;
        isUnbounded=false;  		 	
        variableMap =  new Hashtable<String, Double>();
        optimumValue = isMax? DOUBLE_MIN: DOUBLE_MAX;
    }
    
      

    //return true if I am superior to the other solution
    public boolean isBetterThan(Solution anotherSolution, boolean isMaximization) {

        boolean retval ;

        if (  anotherSolution.getIsFeasibleOrOptimal() ) {

            if (this.getIsFeasibleOrOptimal()) {
                //if both solutions are valid, then we must compare the values

                retval = isMaximization ? 
                        (anotherSolution.getOptimumValue()<= this.optimumValue)
                        :(anotherSolution.getOptimumValue()>= this.optimumValue);	

                        if (retval ){
                            System.out.println ("current Solution better "+   " " +this.optimumValue +" " +anotherSolution.getOptimumValue() );
                        }else {					
                            System.out.println ("anotherSolution better "+ " " +anotherSolution.getOptimumValue() + " " +this.optimumValue);
                        }

            } else {
                //if I am not valid , then any   valid solution is better than I
                retval = false;
            }

        }else{
            //I am always better than an invalid solution
            retval=true;
        }

        return retval;
    }

    //getters and setters and other methods

    public String toString (){
        String result = "No feasible solution is known.";
        if (this.getIsFeasibleOrOptimal()){
            result ="The optimum is : " + this.getOptimumValue() +"\n" + variableMap.toString();
        }
        return result;
    }

    public double getOptimumValue(){
        return optimumValue ;
    }

    public void setOptimumValue(double val){
        optimumValue=val ;
    }

    public boolean getIsFeasibleOrOptimal(){
        return isFeasible || isOptimal;
    }

    public boolean getIsFeasible(){
        return isFeasible ;
    }

    public void setIsFeasible(boolean feasible){
        isFeasible=feasible;
    }
    public boolean getIsOptimal(){
        return this.isOptimal;
    }

    public void setIsOptimal(boolean val){
        this.isOptimal=val;
    }

    public void setIsError(boolean err){
        this.isError=err;
    }
    public boolean getIsError(   ){
        return this.isError;
    }

    public void setIsUnbounded(boolean unbounded){
        isUnbounded=unbounded;
    }

    public boolean getIsUnbounded(){
        return isUnbounded;
    }

    //set value for variable
    public void setVariableValue(String name, double val){
        variableMap.put(name, val);
    }
    public double getVariableValue(String name){
        return variableMap.get(name);
    }

    public Map<String, Double> getAllVariableValues(){
        //return read only copy 
        return  Collections.unmodifiableMap(variableMap);
    }	
    public static Solution fromJSONString(String json){
        return (new Gson()).fromJson(json.trim(), Solution.class);
    }

    public String toJSONString(){
        return(new Gson()).toJson(this);
    }


}
