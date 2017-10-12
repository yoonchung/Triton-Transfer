package metadataServer;

import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;

import java.util.*;
import java.io.*;
import java.lang.String;

public class MetadataServerServer {
   static int id;
   static int m;
   static int metaport;
   static int blockport;
   public static void StartsimpleServer(metadataServer.MetadataServerService.Processor<MetadataServerHandler> processor) {
      try {
         TServerTransport serverTransport = new TServerSocket(metaport);
         //TServer server = new TSimpleServer(
         //   new Args(serverTransport).processor(processor));

         // Use this for a multithreaded server
         TServer server = new TThreadPoolServer(new
            TThreadPoolServer.Args(serverTransport).processor(processor));

         System.out.printf("Starting the simple MetadataServer connection with port: %d...\n", metaport);
         server.serve();
      } catch (Exception e) {
         e.printStackTrace();
      }
   }
 
   public static void main(String[] args) {
/*
    M: 1
    metadata1: <port>, 7878
    block: <port>, 7979

   ./runClient.sh <config_file> <base_dir> <command> <filename>
   ./runMetaServer.sh <config_file> <id>
   ./runBlockServer.sh <config_file>


   Runnable simple = new Runnable() {
      public void run() {
         MetadataServerHandler handler = new MetadataServerHandler(blockport);
         StartsimpleServer(new metadataServer.MetadataServerService.Processor<MetadataServerHandler>(handler));
      }
   };
   //TODO handle port number
   for (int i = 0; i < M; i++) {
      new Thread(simple).start();
      new Thread(simple).start();
      new Thread(simple).start();
   }
   

*/
      String conf = args[0];
      id = Integer.parseInt(args[1]);
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
      String arg[] = new String[argList.size()];
      for (int i = 0; i < argList.size(); i++) {
         String[] s = argList.get(i).split(": ");
         arg[i] = s[1];
      }
      m = Integer.parseInt(arg[0]);
      metaport = Integer.parseInt(arg[1]);
      blockport = Integer.parseInt(arg[2]);

      MetadataServerHandler handler = new MetadataServerHandler(blockport);
      StartsimpleServer(new metadataServer.MetadataServerService.Processor<MetadataServerHandler>(handler));
   }
}
