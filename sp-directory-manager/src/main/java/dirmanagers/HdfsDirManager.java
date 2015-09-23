package dirmanagers;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * 
 * @author tamvadss
 * library with methods for HDFS based directory
 */
public class HdfsDirManager implements IDirManager{

    private final String DIR_ROOT = "/SolverDirectory";
     
    private static Configuration conf;
    private static FileSystem fs;
    private static final Logger logger = Logger.getLogger(HdfsDirManager.class);

    final int INVALID_FOLDER_NUM=-1;

    static {

        try {

            conf = new Configuration();
            conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
            conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));

            fs = FileSystem.get(conf);
            //

        } catch (IOException e) {

            logger.error(e);
        }
    }

    public long getLastNonEmptyFolder() throws IllegalArgumentException, IOException {
        long NUM_DIRS= this.getDirectoryCount();

        long result = INVALID_FOLDER_NUM;

        for ( long index = NUM_DIRS-1; index >=0; index = index -1){
            if (!isEmpty(index)) {
                result= index ;
                break;
            }
        }

        return result ;
    }

    public boolean isEmpty(long depth) throws IllegalArgumentException, IOException {
        String folder = getFolderName(depth);
        ContentSummary cs = fs.getContentSummary(  new Path(folder) );
        return cs.getFileCount()==0;
    } 



    public boolean isEmpty() throws IllegalArgumentException, IOException {		
        return !( INVALID_FOLDER_NUM<getLastNonEmptyFolder());
    }

    public long getFirstNonEmptyFolder() throws IllegalArgumentException, IOException {
        final long NUM_DIRS = this.getDirectoryCount();

        long result = INVALID_FOLDER_NUM;

        for (int index = 0 ; index <NUM_DIRS; index ++){
            if (!isEmpty(index)) {
                result= index ;
                break;
            }
        }
        return result;
    }

    public long getNumFilesInFolder(long depth) throws IllegalArgumentException, IOException {
        ContentSummary cs = fs.getContentSummary(  new Path(getFolderName(depth) ) );
        return cs.getFileCount();
    }

    public long getNumFilesInFolder(String foldername)
            throws IllegalArgumentException, IOException {
        ContentSummary cs = fs.getContentSummary(  new Path(foldername ) );
        return cs.getFileCount();
    }

    public long getNumFilesTotal() throws IllegalArgumentException, IOException {
        long sum = 0;

        for (int index =0 ; index < getDirectoryCount(); index ++){
            sum += getNumFilesInFolder(  index) ;
        }

        return sum;
    }

    public boolean clearAllFoldersExceptRoot( ) {
        boolean result = true;
        final int ROOT_INDEX = 0;
        try {
            final long NUM_DIRS = this.getDirectoryCount();
            for (int index =0 ; index < NUM_DIRS; index ++){
                if (ROOT_INDEX !=index ){
                    clearFolder(this.getFolderName (  index)) ;
                } 				
            }
        } catch (Exception e) {
            logger.error(e);
            result = false;
        }
        return result;
    }

    public boolean clearFolder(String name) {
        boolean result = true;
        try {
            Path path = new Path(name);			
            fs.delete( path, true);
            fs.mkdirs(path);
        } catch (IOException e) {
            logger.error(e);
            result=false;
        }
        return result;
        //return FileUtil.fullyDeleteContents( new File(getFolderName(depth)));		 
    }

    //Warning: this function should ONLY be used by each map task, with a
    //unique file name per task (such as job id + map task id)
    public boolean appendToFile(String foldername, String filename, List<String> contentList)  {

        boolean isAdded = true;

        try {
            Path path = new Path(foldername + "/" + filename);		

            if (!fs.exists(path)) {								
                fs.createNewFile(path );
            }			

            BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.append(path)));     

            for (String content :contentList ){
                br.write(content+"\n");
            }

            br.close();			 

        } catch (IOException e) {
            logger.error(e);
            isAdded= false;
        }

        return isAdded;





    }

    //number of folder under root
    public long getDirectoryCount  () throws IllegalArgumentException, IOException{
        ContentSummary cs = fs.getContentSummary(  new Path(DIR_ROOT) );

        return -1 + cs.getDirectoryCount();
    }


    public String getFolderName(long depth) {

        return DIR_ROOT + "/" + depth;
    }



}
