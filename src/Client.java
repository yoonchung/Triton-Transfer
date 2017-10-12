import shared.*;
import blockServer.*;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;

import java.io.*;
import java.util.*;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Client {
   static int m;
   static int metaport;
   static int blockport;
   static final int BLOCKSIZE = 1024 * 1024 * 4;
   static String base_dir;
   static String command;
   static String filename;
   static TTransport transport;
   static metadataServer.MetadataServerService.Client metaClient;
   static HashMap<String, ArrayList<String>> filedict;
   static HashMap<String, hashBlock> hashdict;

   public static void main(String [] args) {

      String conf = args[0];
      File file = new File(conf);
      ArrayList<String> argList = new ArrayList<String>();
      String line = null;
      try {
         BufferedReader reader = new BufferedReader(new FileReader(file));
         while ((line = reader.readLine()) != null) {
            argList.add(line);
         }
      } catch (Exception e) {
         e.printStackTrace();
      }
      String configArgs[] = new String[argList.size()];
      for (int i = 0; i < argList.size(); i++) {
         String[] s = argList.get(i).split(": ");
         configArgs[i] = s[1];
      }
      m = Integer.parseInt(configArgs[0]);
      metaport = Integer.parseInt(configArgs[1]);
      blockport = Integer.parseInt(configArgs[2]);

      base_dir = args[1];
      command = args[2];
      filename = args[3];

      try {
         TTransport transport;
         //System.out.printf("Starting the simple client connection to 
         //                   metaServer with port: %d...\n", metaport);
         transport = new TSocket("localhost", metaport);
         transport.open();

         TProtocol protocol = new TBinaryProtocol(transport);
         metaClient = new metadataServer.MetadataServerService.Client(protocol);

         perform();

         transport.close();
      } catch (TException x) {
         x.printStackTrace();
      } 
   }

   private static void perform() throws TException {
      System.out.printf("perform()\n");
      // Keeping a list of locally present blocks
      scanFiles();
      //mergeBlock();
      if (command.equals("delete")) {
         delete();
      }
      else if (command.equals("upload")) {
         upload();
      }
      else if (command.equals("download")) {
         download();
      }
   }
   
   private static void delete() throws TException {
      //System.out.printf("delete()\n");
      file f = new file();
      f.filename = filename;
      response r = new response();
      f.version = 1;
      r = metaClient.deleteFile(f);
      if (r.message == responseType.OK)
         System.out.printf("OK\n");
      else 
         System.out.printf("ERROR\n");
   }
   
   private static void upload() throws TException {
      //System.out.printf("upload() %s\n", base_dir + "/" + filename);
      File uploadFile = new File(base_dir + "/" + filename);
      if (!uploadFile.exists() || !uploadFile.isFile()) {
         System.out.println("ERROR");
         System.err.println("File to upload not exists");
         return;
      }
      ArrayList<hashBlock> byteBlocks = divideBlock(uploadFile);
      ArrayList<String> hashList = makeHash(byteBlocks, uploadFile);
      // call storeFile(file1, hashlist) from metaserver
      file uploadfile = new file();
      uploadResponse rMeta = new uploadResponse();
      uploadfile.filename = filename;
      uploadfile.version = 1;
      uploadfile.hashList = hashList;
      rMeta = metaClient.storeFile(uploadfile);
      //TODO: if rMeta has empty hashList, no need to upload
      //System.out.printf("\tupload() rMeta size() is %d.\n", rMeta.hashList.size());
      if (rMeta.hashList.size() != 0) {
         //System.out.printf("\tcalling uploadBlock().\n");
         response blockResponse = uploadBlocks(byteBlocks, hashList, rMeta);
         // call storeFile again to check if the blocks are there
         rMeta = metaClient.storeFile(uploadfile);
         if (rMeta.status == uploadResponseType.ERROR) {
            System.out.println("ERROR");
            System.err.println("Failed to store block on metaServer");
         }
      }
      // Successo
      if (rMeta.status == uploadResponseType.OK) {
         System.out.printf("OK\n", filename);
      }
   }

   private static ArrayList<hashBlock> divideBlock(File uploadFile) throws TException {
      // divide file into blocks
      ArrayList<hashBlock> byteBlocks = new ArrayList<hashBlock>();
      //hashBlocks byteBlocks = new hashBlocks();
      int fileLength = (int)uploadFile.length();
      int numBlock = (fileLength / BLOCKSIZE) + 1;
      if (fileLength > Integer.MAX_VALUE) {
         // File is too large
         throw new TException("File is too large!");
      }
      InputStream is = null;
      byte[] bytes;
      ByteBuffer buf;
      int offset = 0;
      int numReadTotal = 0;
      try {
         is = new FileInputStream(uploadFile);
      } catch (FileNotFoundException e) {
         e.printStackTrace();
      }
      for (int i = 0; i < numBlock; ++i) {
         // Read in the bytes
         offset = 0;
         int numRead = 0;
         // Create the byte array to hold the data
         if ((fileLength - numReadTotal) < BLOCKSIZE)
            bytes = new byte[fileLength - numReadTotal];
         else
            bytes = new byte[BLOCKSIZE];
         try {
            while (offset < bytes.length && 
                  (numRead = is.read(bytes, offset, bytes.length-offset)) >= 0) {
               offset += numRead;
            } 
         } catch (IOException e) {
               e.printStackTrace();
         }
         hashBlock block = new hashBlock();
         //System.out.printf("\t\tSplitting block %d size of %d.\n", i+1, bytes.length);
         buf = ByteBuffer.wrap(bytes);
         block.block = buf;
         byteBlocks.add(i, block);
         numReadTotal += offset;
      }
      try {
         is.close();
      } catch (IOException ex) {
         ex.printStackTrace();
      }
      // Ensure all the bytes have been read in
      if (numReadTotal < fileLength) {
         throw new TException("Could not completely read file " + uploadFile.getName());
      }
      return byteBlocks;
   }
   
   private static ArrayList<String> makeHash(ArrayList<hashBlock> byteBlocks, File uploadFile) throws TException {
      // Fill hashList
      ArrayList<String> hashList = new ArrayList<String>();
      MessageDigest md = null;
      FileInputStream fis = null;
      try {
         md = MessageDigest.getInstance("SHA-256");
      } catch (NoSuchAlgorithmException e) {
         e.printStackTrace();
      }
      try {
         fis = new FileInputStream(base_dir + "/" + uploadFile.getName());
      } catch (FileNotFoundException e) {
         e.printStackTrace();
      }
      for (int i = 0; i < byteBlocks.size(); ++i) {
         int blockSize = byteBlocks.get(i).block.array().length;
         byte[] dataBytes = new byte[blockSize];
         dataBytes = byteBlocks.get(i).block.array();
         md.update(dataBytes, 0, blockSize);
         byte[] mdbytes = md.digest();
         //convert the byte to hex format method 2
         StringBuffer hexString = new StringBuffer();
         for (int j = 0; j < mdbytes.length; j++) {
            hexString.append(Integer.toHexString(0xFF & mdbytes[j]));
         }
         byteBlocks.get(i).hash = hexString.toString();
         hashList.add(hexString.toString());
      }
      return hashList;
   }

   private static response uploadBlocks(ArrayList<hashBlock> byteBlocks, ArrayList<String> hashList, 
      uploadResponse rMeta) throws TException {
      //System.out.printf("uploadBlocks() with %d byteBlocks and %d hashList\n", 
      //                   byteBlocks.size(), hashList.size());
      // HashMap to avoid duplicated block upload
      HashMap<String, hashBlock> uploadBlocks = new HashMap<String, hashBlock>();
      for (int i = 0; i < byteBlocks.size(); ++i) {
         uploadBlocks.put(hashList.get(i), byteBlocks.get(i));
      }
      // call storeBlock() from blockserver
      response rBlock = new response();
      blockServer.BlockServerService.Client blockClient = connectBlockServer();
      if (blockClient == null) {
         //f.status = responseType.ERROR;
         System.out.println("ERROR");
         System.err.println("Cannot connect to blockServer");
         return null; // f;
      }
      for (String key : uploadBlocks.keySet()) {
         rBlock = blockClient.storeBlock(uploadBlocks.get(key));
         if (rBlock.message == responseType.ERROR) {
            System.out.println("ERROR");
            System.err.println("Failed to store block on blockServer");
            return rBlock;
         }
      }

      closeBlockServer();
      return rBlock;
   }

   private static void scanFiles() throws TException {
      //System.out.printf("scanFiles() \n");
      File baseDir = new File(base_dir);
      File[] files = baseDir.listFiles();
      // (filename, hashList)
      filedict = new HashMap<String, ArrayList<String>>();
      // (hash, hashBlock)
      hashdict = new HashMap<String, hashBlock>();
      ArrayList<hashBlock> blocklist = new ArrayList<hashBlock>();
      ArrayList<String> hashlist = new ArrayList<String>();
      for (File f : files) {
         //System.out.printf("\tscanFiles() scanning %s\n", f.getName());
         if (f.isFile()) {
            blocklist = divideBlock(f);
            hashlist = makeHash(blocklist, f);
            filedict.put(f.getName(), hashlist);
            for (int i = 0; i < blocklist.size(); ++i) {
               //System.out.printf("\t\t%s, hash: %s\n", f.getName(), hashlist.get(i));
               hashdict.put(hashlist.get(i), blocklist.get(i));
            }
         }
      }
      //System.out.printf("scanFiles() done. \n\n");
   }

   private static void download() throws TException {
      //System.out.printf("download() %s\n", filename);
      // get blocklist from metaserver and populate missing block list
      file metaf = metaClient.getFile(filename);
      if (metaf.status == responseType.ERROR) {
         System.out.println("ERROR");
         System.err.println("The file does not exist on metaserver");
         return;
      }

      ArrayList<String> missingList = new ArrayList<String>();
      for (int i = 0; i < metaf.hashList.size(); i++) {
         if (!hashdict.containsKey(metaf.hashList.get(i))) {
            missingList.add(metaf.hashList.get(i));
         }
      }

      // download missing blocks from blockserver and put them in hashdict
      blockServer.BlockServerService.Client blockClient = connectBlockServer();
      //System.out.printf("Client: downloading %d block(s) of %s from blockServer.\n", missingList.size(), filename);
      if (blockClient == null) {
         System.out.println("ERROR");
         System.err.println("Cannot connect to blockServer");
         return;
      }
      hashBlock[] blocks = new hashBlock[missingList.size()];
      for (int i = 0; i < missingList.size(); ++i) {
         blocks[i] = blockClient.getBlock(missingList.get(i));
         if (blocks[i] == null) {
            System.out.println("ERROR");
            System.err.println("Cannot download from blockServer");
            return;
         }
         hashdict.put(missingList.get(i), blocks[i]);
      }
      closeBlockServer();

      // merge blocks to write file
      FileOutputStream stream = null;
      ByteBuffer buf = null;
      byte[] bytes;
      for (int i = 0; i < metaf.hashList.size(); ++i) {
         try {
            bytes = hashdict.get(metaf.hashList.get(i)).block.array();
            stream = new FileOutputStream(filename, true);
            //System.out.printf("Merging block %d of %d, size of %d with hash %s.\n", i+1,
               //metaf.hashList.size(), bytes.length, metaf.hashList.get(i));
            stream.write(bytes);
            stream.flush();
         } catch (IOException e) {
            e.printStackTrace();
         } finally {
            try {
               if (stream != null)
                  stream.close();
            } catch (IOException e) {
            e.printStackTrace();
            }
         }
      }
      System.out.printf("OK\n", filename);
   }

   public static blockServer.BlockServerService.Client connectBlockServer() {
      blockServer.BlockServerService.Client blockClient = null;
      try {
         //System.out.printf("Starting a simple client connection to blockServer with port: %d...\n", blockport);
         transport = new TSocket("localhost", blockport);
         transport.open();
         TProtocol protocol = new TBinaryProtocol(transport);
         blockClient = new blockServer.BlockServerService.Client(protocol);
      } catch (TException x) {
         x.printStackTrace();
      }
      return blockClient;
   }

   public static void closeBlockServer() {
      //System.out.printf("Closing a simple client connection to blockServer with port: %d...\n", blockport);
       transport.close();
   }
}
