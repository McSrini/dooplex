package drivers;

import static constantsAndParams.Constants.SAV_FILENAME;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;

import dataTypes.ActiveSubtree;
import dataTypes.NodeAttachment;
import ilog.concert.IloException;
import ilog.cplex.IloCplex;
import static constantsAndParams.Constants.*;
import static constantsAndParams.Parameters.*;

public class SimpleDriver {
    
    private static int treesLeft ( List<ActiveSubtree> activeSubtreeList) throws IloException {
        int TREES_LEFT=  ZERO;
        for (int index = ZERO ; index < activeSubtreeList.size(); index ++){
            ActiveSubtree tree = activeSubtreeList.get(index);
            if (tree.getSolution().isOptimal()) continue ;
            if (tree.getSolution().isUnFeasible()) continue ;
            TREES_LEFT++;
        }
        return TREES_LEFT;
    }

    public static void main(String[] args) {
        
        double bestKnownIncumbentValue = PLUS_INFINITY;
        int bestKnownIncumbentIndex = -ONE;
        Random randomGenerator = new Random(ONE);
       
        try {
            
            System.out.println("Started at "+LocalDateTime.now());
            
            List<ActiveSubtree> activeSubtreeList = new ArrayList<ActiveSubtree>();
            activeSubtreeList.add(new ActiveSubtree( new NodeAttachment()));
            
            int treesleft=treesLeft (  activeSubtreeList);
            while (ZERO < treesleft) {
                
                List<NodeAttachment> farmedOutNodes = new ArrayList<NodeAttachment>();
                
                //solve for some time
                for (int index = ZERO ; index < activeSubtreeList.size(); index ++){
                    ActiveSubtree mip = activeSubtreeList.get(index);
                    if (mip.getSolution().isOptimal()) continue ;
                    if (mip.getSolution().isUnFeasible()) continue ;
                    if (mip.isEntireSubtreeDiscardable()) continue ;
                   
                    farmedOutNodes.addAll(
                            mip. solve (   THOUSAND*SIX,   false,   bestKnownIncumbentValue   ,true 
                                     /*randomGenerator.nextInt(TWO) < ONE  */  )
                            );
                }
                
                //update best known incumbent and its index
                for (int index = ZERO ; index < activeSubtreeList.size(); index ++){
                    ActiveSubtree mip = activeSubtreeList.get(index);
                    if (mip.getSolution().isOptimal() && mip.getSolution().getObjectiveValue()<bestKnownIncumbentValue) {
                        bestKnownIncumbentIndex = index;
                        bestKnownIncumbentValue = mip.getSolution().getObjectiveValue();
                    }
                }
                
                //  convert   farmed out nodes to AST, and add to AST-list
                for (int index = ZERO ; index < farmedOutNodes.size(); index ++){
                    NodeAttachment node = farmedOutNodes.get(index);
                    activeSubtreeList.add(new ActiveSubtree(node));
                }
                
                treesleft=treesLeft (  activeSubtreeList);
                System.out.println("best Incumbent "+bestKnownIncumbentValue);
                System.out.println("Active subtrees left "+treesleft);
            }
            
            
            
            System.out.println( activeSubtreeList.get(bestKnownIncumbentIndex).getSolution().toString());
            
            System.out.println("Completed at "+LocalDateTime.now());
            
            
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        

    }

}

/**
 * 
 * 
 
0029 1.0
0028 0.0
0027 1.0
0026 1.0
0025 1.0
0024 0.0
0023 1.0
0022 1.0
0021 1.0
0020 0.0
0019 0.0
0018 1.0
0017 0.0
0016 1.0
0015 1.0
0014 0.0
0045 1.0
0013 0.0
0044 0.0
0012 0.0
0043 1.0
0011 1.0
0042 1.0
0010 1.0
0041 1.0
0040 1.0
0009 1.0
0008 1.0
0039 1.0
0007 -0.0
0038 0.0
0006 1.0
0037 1.0
0005 1.0
0036 0.0
0004 1.0
0003 1.0
0035 1.0
0002 1.0
0034 1.0
0001 1.0
0033 0.0
0032 0.0
0031 0.0
0030 1.0
 
 * 
 */

