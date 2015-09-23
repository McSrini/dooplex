package drivers;

import ilog.concert.IloException;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer; 
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import clients.ConfigClient;
import dirmanagers.HdfsDirManager;
import dirmanagers.IDirManager;
import server.ActiveKeyValueStore;
import solverTypes.NodeAttachment;
import solverTypes.Solution;
import solvers.CPSolver;

/**
 * 
 * @author tamvadss
 * Driver class for coordinating distributed branch and bound on Hadoop, using a CPLEX solver
 */
public class HDFSDriver extends Configured implements Tool {

    private static final Logger logger = Logger.getLogger(HDFSDriver.class);

    final static String ZOO_SERVER = "ip-172-31-47-64.ec2.internal";


    //A handle to HDFS, used to read results of reduction
    //Note that we have a separate directory manager for reading the "CPLEX tree nodes"
    private static  FileSystem fs =null;	

    //Folder where reduced solutions are put
    final String OUTPUT_DIR = "/user/ubuntu/testing/wordcount/output";

    //define a constant key string used to emit solutions and new nodes
    final static Text CONSTANT_KEY_STRING_SOLN = new Text("SOLN");
    //note that newly generated nodes are emitted with their depth as the key

    // the original problem file name which must be on local disk of every machine, and whether
    // this is a maximization, must both be supplied with -D option with these keys
    // 
    static final String IS_MAXIMIZATION = "ismax"  ; 	
    static final String BEST_KNOWN_OPTIMUM = "BEST_KNOWN_OPTIMUM"  ;      
    static final String ORIGINAL_LP_FILE = "lpfile";

    static final int NUM_MAPS_PER_WORKER = 4 ;
    static final int NUM_WORKERS = 3 ; 
    static final int NUM_FILES_PER_FOLDER = NUM_WORKERS*NUM_MAPS_PER_WORKER ; //number of workers * cores per worker


    static{

        Configuration conf = new Configuration();
        try {
            //initialize the handle
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            logger.error(e);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new HDFSDriver(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception { 	
              
        //initialize the following things we will need in our iterative solution

        //This is the interface to the directory of CPLEX nodes
        IDirManager dirManager = new HdfsDirManager();

        int iterationCount = 0;
        final int MAX_ITER_COUNT = 100 ; //set an upper limit
        int exitCode =0;

        //this is used for statistics
        //int numberofFilesGenerated = 0;
        Date startTime = new Date();

        //check if -D ismax=false
        boolean isMaximization = this.getConf().get(IS_MAXIMIZATION).equalsIgnoreCase("TRUE");
        
        //start with an infeasible solution as the current best solution
        Solution currentBestSolution = new Solution(isMaximization);

        //create a zoo keeper node
        //ConfigUpdater configUpdater = new ConfigUpdater(ZOO_SERVER  ); 
        //configUpdater.update("3");
        //now register for updates on this value
        //ConfigWatcher watcher = new ConfigWatcher(ZOO_SERVER);

        //check existence -D lpfile=/tmp/stein15.lp
        if ( ! localFileExists(this.getConf().get(ORIGINAL_LP_FILE))) {

            logger.error("Unable to find lp file " + this.getConf().get(ORIGINAL_LP_FILE));
            System.exit(1);    		
        }

        //reset the CPLEX directory in preparation for our iterations
        dirManager.clearAllFoldersExceptRoot();
        
        //start the configuration service
        //startConfigService();
        //prepare the client for future use
        //ConfigClient configClient = new ConfigClient( );

        //we loop until the directory of CPLEX nodes is empty, i.e. all CPLEX nodes have been solved        
        while (! dirManager.isEmpty() && MAX_ITER_COUNT >iterationCount) {

            logger.info("Starting iteration "+iterationCount);
            if (currentBestSolution.getIsOptimal()){
                logger.info("Best known optimum solution so far is " + currentBestSolution.getOptimumValue());
            }else{
                logger.info("No optimum solution is known yet.");
            }

            //we use zoo keeper to keep track of how many workers are still active
            //logger.info("Zoo counter value is = " + configUpdater.read());
            //re initialize counter
            //configUpdater.update("3");

            Configuration conf = this.getConf();      	    
            // Create map reduce job
            Job job = Job.getInstance(conf, "DoopLex");
            job.setJarByClass(this.getClass());
            
            //set the best known optimum so far into the configuration
            //this is used to farm out nodes which are potentially better that the current solution
            //configClient.connect(ZOO_SERVER);
            //configClient.write(Double.toString(currentBestSolution.getOptimumValue()));
            //logger.info("Updated config with best known optimum solution    " + configClient.read());
            //configClient.close();
            
            //we always do a simplified version of BFS right now, so the input folder is the
            //smallest folder from the Directory manager.
            //We can do DFS by selecting the largest folder.
            //Not sure how to do best first.
            //
            //Note that Hadoop allows us to take more than 1 folder as input, if needed.
            //We can also use wild cards to create the input folders
            //
            long   inputFolder =  dirManager.getFirstNonEmptyFolder() ;    
            logger.info("Processing nodes in folder " + inputFolder);

            // Use TextInputFormat to define the input folder for maps
            FileInputFormat.addInputPath(job, new Path( dirManager.getFolderName(inputFolder) ));

            try{
                // delete output folder , true for recursive	
                fs.delete(new Path(OUTPUT_DIR), true); 	
                //logger.debug("deleted  output folder in preparation for next iteration" + OUTPUT_DIR);
            } catch (IOException ioex) {
                logger.error(ioex);
            }

            //set the output folder
            FileOutputFormat.setOutputPath(job, new Path(OUTPUT_DIR));

            job.setMapperClass(Map.class);    	    
            job.setReducerClass(Reduce   .class);
            job.setOutputKeyClass(Text.class);
            //note that the Solution we emit is the solution in JSON string format  
            job.setOutputValueClass(Text.class);

            exitCode= (job.waitForCompletion(true)? 0:1 ) ;	    	

            if(0==exitCode){
                Solution solnFromReduce = getReducedSolution(conf);	
                if (! currentBestSolution  .isBetterThan(solnFromReduce, isMaximization)){

                    //we have found a better optimum
                    currentBestSolution=solnFromReduce;

                    //its possible that both current and new solutions were infeasible
                    if (currentBestSolution.getIsFeasibleOrOptimal()){
                        logger.info("the current best known optimum is ="+ currentBestSolution.getOptimumValue());
                    }    	else{
                        logger.info("no optimum solution as of yet");
                    }
                } else{
                    logger.info("  no better solution found in this iteration  " );
                }    	    	 
            } else {
                logger.error("Map reduce job resulted in error");
                System.exit(exitCode);
            }
            
            //set the current best solution into the configuration
            conf.set(BEST_KNOWN_OPTIMUM, (new Double( currentBestSolution.getOptimumValue())).toString());

            //prepare for next iteration 

            //delete files from folder we just processed, we do not want to process them again
            dirManager.clearFolder(dirManager.getFolderName(inputFolder));

            iterationCount	++;    	    

        } //end while directory not empty

        //print some statistics and exit
        logger.info("Solution found in " + iterationCount + " iterations");
        //logger.info("Number of files generated " + numberofFilesGenerated);
        logger.info("Start time- " + startTime.toString());
        logger.info("End time  - " + (new Date()).toString());
        logger.info("The solution is as follows ->");
        logger.info(currentBestSolution.toString());

        return exitCode;
    }

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {	

        public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
            
            Configuration conf = context.getConfiguration();
            String originalLP_Filename  = conf.get(ORIGINAL_LP_FILE );
            //check if maximization
            boolean isMax = conf.get(IS_MAXIMIZATION).equalsIgnoreCase("TRUE");
            
            //temporarily hard coded
            final int ROOT_TIME_SLICE = 2;
            final int INTERIOR_TIME_SLICE = 60;
            int timeSlice=INTERIOR_TIME_SLICE;
           
            //try to read the best known optimum
            //initialize it to infinity
            //double bestKnownOptimum = 
            
            /*
            //now try to get it from the configuration service
            ConfigClient configClient = new ConfigClient();
            configClient.connect(ZOO_SERVER);
            try {
                bestKnownOptimum = Double.parseDouble(configClient.read());
            } catch ( Exception e1) {
                logger.error(e1);
            }  
            logger.debug(" map best known optimum solution    " + bestKnownOptimum);
            configClient.close();
           */

            //the text of the original problem in the CPLEX root folder will contain this line
            final String ORIGINAL_PROBLEM = "ORIGINAL_PROBLEM";

            //this is the solver we will use to generate solutions and  new nodes
            CPSolver solver = null;         

            //read a line from the input file, and process it to create a solution object
            String line = lineText.toString();
            boolean isThisRootProblem = ORIGINAL_PROBLEM.equalsIgnoreCase(line);
            NodeAttachment nodeAttachment = isThisRootProblem? null: NodeAttachment.fromJSONString(line );
            
            if ( isThisRootProblem ) {
                //root node , no attachment
                solver=	new CPSolver(originalLP_Filename , null , isMax ); 	   
                timeSlice = ROOT_TIME_SLICE;
            } else{
                //this is an interior node, we must pass in the node attachment
                solver = new CPSolver(originalLP_Filename ,nodeAttachment , isMax );  	    	   
            }
            
            //solve this only if parent LP relax is better than current optimum
            if ( true ) {
                
                //solve this node and receive any new nodes created in a list
                List <NodeAttachment> newNodeList = new ArrayList <NodeAttachment>();
                try {

                    Solution subTreeSolution = solver.solve(newNodeList, timeSlice,   
                            isMax? Solution.DOUBLE_MIN: Solution.DOUBLE_MAX );

                    /*
                    //write the new nodes into the CPLEX directory using directory manager
                    IDirManager dirManager = new HdfsDirManager();

                    //make sure file name is unique for a given map task
                    //we can also add the host name although its not required for uniqueness
                    String filename = java.net.InetAddress.getLocalHost().getHostName()+
                            "_" +   context.getTaskAttemptID().toString();
                     */

                    for (NodeAttachment attachment : newNodeList) {
                        //the new CPLEX nodes must be emitted with the correct depth as key
                        context.write(new Text(""+attachment.getDepth()), new Text(attachment.toJSONString() ));                    

                        //dirManager.appendToFile(dirManager.getFolderName(depth), filename, attachment.toJSONString()+"\n");
                    }

                    //emit the sub tree solution
                    context.write(CONSTANT_KEY_STRING_SOLN, new Text(subTreeSolution.toJSONString()));     

                    //inform the driver of map completion
                    //(new ConfigUpdater(ZOO_SERVER)).update(java.net.InetAddress.getLocalHost().getHostName() );

                } catch (Exception e) {
                    //catch each exception individually
                    logger.error(e);
                } //end try catch
                
            } //if LP relax is better

        }//end map method
        
    }//end Map class

    public static class Reduce  extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text word, Iterable<Text> solutions, Context context)    throws IOException, InterruptedException {

            //we reduce solutions as well as new nodes
            boolean isThisSolutionReduction = false;

            //depth of emitted nodes being processed, in case we are processing new nodes
            int depth = 0 ;

            //check if this is a new node list, or solution
            if (word.toString().equalsIgnoreCase(CONSTANT_KEY_STRING_SOLN.toString())) {
                //this is the emitted solution
                isThisSolutionReduction = true;  
            } else {
                //this is a list of new nodes
                depth=Integer.parseInt(word.toString().trim());
            }            

            //check if maximization
            Configuration conf = context.getConfiguration();
            boolean isMax = conf.get(IS_MAXIMIZATION).equalsIgnoreCase("TRUE");
            
            //start with an invalid current solution in case we are reducing solutions
            Solution bestSolutionInThisIteration = new Solution(isMax );

            if (isThisSolutionReduction) {

                //reduce to the best Solution found In This Iteration 
                for (Text solnText : solutions) {	 
                    Solution soln =  Solution.fromJSONString(solnText.toString());
                    if ( soln.getIsFeasibleOrOptimal()) {
                        if (! bestSolutionInThisIteration .isBetterThan(soln, isMax)) {
                            //we have found a better solution
                            bestSolutionInThisIteration= soln;		    		 
                        }
                    }
                }

                context.write(word, new Text(bestSolutionInThisIteration.toJSONString()));

            } else {
                //write the new nodes into the correct folder
                //
                //we exploit the fact that 2 machines  do not both reduce nodes at the same depth
                //
                // the number of files per folder is constant , currently hard coded
                // The newly generated nodes are randomly and evenly distributed among these files


                //we initialize a map of new nodes, which will be appended into the respective files
                java.util.Map<Integer, List<String>> newNodesMap =  new Hashtable<Integer, List<String>>();
                for (int index =0; index < NUM_FILES_PER_FOLDER; index ++){
                    newNodesMap.put(index, new ArrayList<String>());
                }

                for (Text solnText : solutions) {	
                    //add the solution to the map
                    int random = (new Random()).nextInt(NUM_FILES_PER_FOLDER);
                    newNodesMap.get(random).add(solnText.toString());
                }

                //write the new nodes into the CPLEX directory using directory manager
                IDirManager dirManager = new HdfsDirManager(); 
                for (int index =0; index < NUM_FILES_PER_FOLDER; index ++){
                    List<String> contentList = newNodesMap.get(index);
                    if (contentList!=null && !contentList.isEmpty()){
                        //append the content to the file
                        dirManager.appendToFile(dirManager.getFolderName(depth), ""+index+".txt", contentList);
                    }
                }	    

            }//end if reduction then else  	

        }//end function reduce

    }//end class Reduce

    private boolean localFileExists (String filename) {
        File f = new File(filename);
        return (f.exists() && !f.isDirectory());
    }

    private  Solution getReducedSolution(Configuration conf) throws IOException{

        Path path = null;
        Solution result = null;

        FileStatus[] statusAry = fs.listStatus(new Path(OUTPUT_DIR));
        for (FileStatus status : statusAry) {
            if (status.isFile() && status.getBlockSize()>0) {
                path = status.getPath();
            }    	        
        }

        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(path)));
        String line = br.readLine();

        result = Solution.fromJSONString(line.split(CONSTANT_KEY_STRING_SOLN.toString())[1].trim());

        return result;

    }
    
    //return true if node is worthy of solving
    private static boolean compareLPRelaxationToCurrentBest (double lpRelax, double currentBest, boolean isMax) {
        return isMax? (lpRelax>currentBest) : (lpRelax<currentBest) ;
        
    }
    
    /*
    private void startConfigService () throws IOException, InterruptedException {
        
        ActiveKeyValueStore store = new ActiveKeyValueStore();
        store.connect(ZOO_SERVER);
    }
    */
}
