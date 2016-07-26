package callbacks;
 
import dataTypes.NodeAttachment;
import dataTypes.SubtreeMetaData;
import ilog.concert.IloException;
import ilog.cplex.IloCplex;
import static constantsAndParams.Constants.*;
import static constantsAndParams.Parameters.*;

/**
 * 
 * @author srini
 * 
 * records solution start time for this node
 *
 */
public class NodeHandler extends IloCplex.NodeCallback{
    
    //meta data of the subtree which we are monitoring
    private SubtreeMetaData metaData;
    
    public NodeHandler (SubtreeMetaData metaData) {
        this.  metaData= metaData;
    }
 
    protected void main() throws IloException {
        
        if (ZERO<getNremainingNodes64()) {
            
            //get the node data for the node chosen for solving 
            NodeAttachment nodeData = (NodeAttachment) getNodeData(ZERO);
            
            //Mark the solution start time. 
            //Solution time may end up being an overestimate, since it could include spark iteration restart time.
            if (nodeData.getStartTimeFor_LP_Relaxation()<=ZERO) nodeData.setStartTimeFor_LP_Relaxation(System.currentTimeMillis());
                        
            setNodeData(ZERO,nodeData);
            
            //remove this node from the list of unsolved nodes
            metaData.removeUnsolvedLeafNodes(getNodeId(ZERO));
        }
        
    }

}
