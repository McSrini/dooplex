package dirmanagers;

import java.io.IOException;
import java.util.List;

/**
 * 
 * @author tamvadss
 * methods for interacting with the directory
 */
public interface IDirManager {

    public boolean isEmpty() throws IllegalArgumentException, IOException;

    public boolean isEmpty(long depth) throws IllegalArgumentException, IOException;

    // delete contents of folder J
    public boolean clearFolder( String name) ;
    public boolean clearAllFoldersExceptRoot(   ) ;

    //the name of the folder at depth J
    public String getFolderName( long depth);

    //the number of items in folder j
    public long getNumFilesInFolder( long depth) throws IllegalArgumentException, IOException;

    //the number of items in the whole directory
    public long getNumFilesTotal(  ) throws IllegalArgumentException, IOException;

    public long getNumFilesInFolder( String foldername ) throws IllegalArgumentException, IOException;

    //FirstNonEmptyFolder
    public long getFirstNonEmptyFolder(  ) throws IllegalArgumentException, IOException;

    //last  NonEmptyFolder, this is the current depth of the solution tree
    public long getLastNonEmptyFolder(  ) throws IllegalArgumentException, IOException;

    //insert an item into folder j
    public boolean appendToFile(String folder, String filename, List<String> contentList)  ;

    //other methods 
    public long getDirectoryCount () throws IllegalArgumentException, IOException ;

}
