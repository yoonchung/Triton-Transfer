package metadataServer;

import org.apache.thrift.TException;

import shared.*;
import java.util.*;
import java.nio.ByteBuffer;

import blockServer.*;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;

public class MetadataServerHandler implements MetadataServerService.Iface {
   static HashMap<String, file> files = new HashMap<String, file>();
   int blockport;
   TTransport transport;
   public MetadataServerHandler(int blockport) {
        this.blockport = blockport;
   }

    @Override
    public file getFile(String filename)
        throws org.apache.thrift.TException
    {
        System.out.println("getFile()");
        System.out.println("  got file w/ name " + filename);

        if (!files.containsKey(filename)) {
            file f = new file();
            f.status = responseType.ERROR;
            return f;
        }
        file f = files.get(filename);
        return f;
    }

    @Override
    public uploadResponse storeFile(file f)
        throws org.apache.thrift.TException
    {
        System.out.printf("storeFile() for %s\n", f.filename);
        if (files.containsKey(f.filename)) {
            file serverFile = files.get(f.filename);
            /*
             * versioning is not required for part 1
             *
            // TODO: accomodate with the version number
            if (serverFile.version >= f.version) {
                response.status = uploadResponseType.FILE_ALREADY_PRESENT;
            }
            */
            return findMissingBlock(f);
        }
        else {
        	return findMissingBlock(f);
        }
    }

    @Override
    public response deleteFile(file f)
        throws org.apache.thrift.TException
    {
        System.out.println("deleteFile()");
        response r = new response();
        if (files.remove(f.filename) != null)
            r.message = responseType.OK;
        else 
            r.message = responseType.ERROR;
        return r;
    }

    public uploadResponse findMissingBlock(file f) {
        System.out.printf("findMissingBlock() for %s\n", f.filename);
    	uploadResponse response = new uploadResponse();
        response.hashList = new ArrayList<String>();
    	blockServer.BlockServerService.Client client = connectBlockServer();
        if (client == null) {
            response.status = uploadResponseType.ERROR;
            return response;
        }
        hashBlock block; 

        System.out.printf("\t%s has %d blocks\n", f.filename, f.hashList.size());
        for (int i = 0; i < f.hashList.size(); i++) {
            //TODO: getting TException wo try, why?
            System.out.printf("\t%d of %d block has hash:\n\t%s.\n", i+1, f.hashList.size(), f.hashList.get(i));
            try {
                boolean hasBlock = client.hasBlock(f.hashList.get(i));
                if (!hasBlock) {
                    response.hashList.add(f.hashList.get(i));
                }
            } catch (TException x) {
                x.printStackTrace();
            }
        }
        closeBlockServer();
        if (response.hashList.size() > 0) {
            System.out.printf("\tMissing %d blocks out of %d.\n", response.hashList.size(), f.hashList.size());
            response.status = uploadResponseType.MISSING_BLOCKS;
        }
        else {
            response.status = uploadResponseType.OK;
            files.put(f.filename, f);
            System.out.printf("\tAdding metadata, total metadata size is %d\n", files.size());
            System.out.printf("\tMetaServer has %s\n", files.keySet());
        }
        return response;
    }

    public blockServer.BlockServerService.Client connectBlockServer() {
        blockServer.BlockServerService.Client blockClient = null;
        try {
            transport = new TSocket("localhost", blockport);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            blockClient = new blockServer.BlockServerService.Client(protocol);
            System.out.printf("Starting the simple BlockServer connection with port: %d...\n", blockport);
        } catch (TException x) {
            x.printStackTrace();
        }
        return blockClient;
    }

    public void closeBlockServer() {
        System.out.printf("Closing the simple BlockServer connection with port: %d...\n", blockport);
        transport.close();
    }
}
