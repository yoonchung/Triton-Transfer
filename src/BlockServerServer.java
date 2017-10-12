package blockServer;

import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;

import java.util.*;
import java.io.*;

public class BlockServerServer {
   static int m;
   static int metaport;
   static int blockport;

   public static void StartsimpleServer(blockServer.BlockServerService.Processor<BlockServerHandler> processor) {
      try {
         TServerTransport serverTransport = new TServerSocket(blockport);
         //TServer server = new TSimpleServer(
         //   new Args(serverTransport).processor(processor));

         // Use this for a multithreaded server
         TServer server = new TThreadPoolServer(new
            TThreadPoolServer.Args(serverTransport).processor(processor));

         System.out.printf("Starting the simple BlockServer with port: %d...\n", blockport);
         server.serve();
      } catch (Exception e) {
         e.printStackTrace();
      }
   }
 
   public static void main(String[] args) {
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
      String arg[] = new String[argList.size()];
      for (int i = 0; i < argList.size(); i++) {
         String[] s = argList.get(i).split(": ");
         arg[i] = s[1];
      }
      m = Integer.parseInt(arg[0]);
      metaport = Integer.parseInt(arg[1]);
      blockport = Integer.parseInt(arg[2]);

      BlockServerHandler handler = new BlockServerHandler();
      StartsimpleServer(new blockServer.BlockServerService.Processor<BlockServerHandler>(handler));
   }
}
