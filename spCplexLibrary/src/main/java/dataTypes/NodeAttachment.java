package dataTypes;

import java.io.Serializable;
import java.util.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static constantsAndParams.Constants.*;
 

/**
 * 
 * @author srini
 *
 * This object is attached to every tree node, and can be used to reconstruct migrated leaf nodes.
 * 
 * It contains branching variable bounds, and other useful information.
 * 
 */
public class NodeAttachment implements Serializable{

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    // distance From Original Node    , never changes
    protected int distanceFromOriginalRoot =ZERO;   
    //    this is  the depth in the current subtree   , may change to 0 if node is migrated
    protected int distanceFromSubtreeRoot=ZERO;   

    //every time there is a branching on a variable, we update on of these lists with the
    //new bound corresponding to the branching condition
    //
    //the key is the Variable name
    //
    //Note that this list may also have bounds on non-branching variables 
    //
    protected Map< String, Double > upperBounds  = new HashMap< String, Double >();
    protected Map< String, Double > lowerBounds = new HashMap< String, Double >();
    
    //easy nodes are close to being solved
    protected boolean isEasy = false;
    
    //record time for LP  relaxation in milliseconds
    //we use System.currentTimeMillis() to measure time taken
    protected double startTimeFor_LP_Relaxation_millisec= ZERO;
    protected double endTimeFor_LP_Relaxation_millisec= ZERO;
    
    //constructors    
    public NodeAttachment () {
      
    }
    
    public NodeAttachment ( boolean easy,  Map< String, Double > upperBounds, 
            Map< String, Double > lowerBounds,  int distanceFromOriginalRoot, int distanceFromSubtreeRoot) {
         
        this.upperBounds = new HashMap< String, Double >();
        this.lowerBounds = new HashMap< String, Double >();
        for (Entry <String, Double> entry : upperBounds.entrySet()){
            this.upperBounds.put(entry.getKey(), entry.getValue()  );
        }
        for (Entry <String, Double> entry : lowerBounds.entrySet()){
            this.lowerBounds.put(entry.getKey(), entry.getValue()  );
        }
        
        this.isEasy = easy;
        this.distanceFromOriginalRoot=distanceFromOriginalRoot;
        this.distanceFromSubtreeRoot=distanceFromSubtreeRoot;
    }
    
    public String toString() {
        String result = distanceFromOriginalRoot + NEWLINE;
        result += distanceFromSubtreeRoot+ NEWLINE;
        for (Entry entry : upperBounds.entrySet()) {
            result += entry.getKey()+BLANKSPACE + entry.getValue()+ NEWLINE;
                    
        }
        for (Entry entry : lowerBounds.entrySet()) {
            result += entry.getKey()+BLANKSPACE + entry.getValue()+ NEWLINE;
                    
        }
        return result;
    }
    
    public int getDepthFromOriginalRoot(  ){
        return distanceFromOriginalRoot  ;
    }
    
    public void setDepthFromSubtreeRoot(int depth ){
        this.distanceFromSubtreeRoot = depth;
    }
    
    public int getDepthFromSubtreeRoot(){
        return distanceFromSubtreeRoot  ;
    }
    
    public double getTimeFor_LP_Relaxation() {
        return  this.endTimeFor_LP_Relaxation_millisec-this.startTimeFor_LP_Relaxation_millisec;
    }

    public double getStartTimeFor_LP_Relaxation() {        
        return this.startTimeFor_LP_Relaxation_millisec;
    }
    public double getEndTimeFor_LP_Relaxation() {        
        return this.endTimeFor_LP_Relaxation_millisec;
    }
    
    public void setStartTimeFor_LP_Relaxation(double time) {
        startTimeFor_LP_Relaxation_millisec = time;
    }

    public void setEndTimeFor_LP_Relaxation(double time) {
        endTimeFor_LP_Relaxation_millisec = time;
    }
    
    public void setEasy(){
        isEasy = true;
    }
    
    public boolean isEasy(){
        return isEasy  ;
    }

    public Map< String, Double >   getUpperBounds   () {
        return  upperBounds ;
    }

    public Map< String, Double >   getLowerBounds   () {
        return  lowerBounds ;
    }

    
    
}
