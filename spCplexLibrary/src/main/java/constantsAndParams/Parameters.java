package constantsAndParams;

import java.io.Serializable;
import static constantsAndParams.Constants.*;
 

public class Parameters implements Serializable{
    
    //should move to properties file
    
    //do not migrate node if its LP relax has taken longer than this
    public static double  LP_RELAX_THRESHOLD_FOR_FARMING_MILLISEC = THOUSAND*THOUSAND  ; //17 MINUTES
    
    //do not allow any subtree to grow bigger than this
    public static double  MAX_LEAFS_PER_SUBTREE =  THOUSAND*THOUSAND ;
    
    public static double  RELATIVE_MIP_GAP = ZERO;
    
    //search strategy
    public static boolean  DEPTH_FIRST_SEARCH = false;
    
    //the partition on which this library (i.e. the ActiveSubtree and supporting objects) live
    public static int  PARTITION_ID = ZERO;
    
}
