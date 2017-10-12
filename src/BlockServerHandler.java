package blockServer;

import org.apache.thrift.TException;

import blockServer.hashBlock;
import shared.*;

import java.util.*;
import java.nio.ByteBuffer;

public class BlockServerHandler implements BlockServerService.Iface {
    static HashMap<String, hashBlock> hashblocks = new HashMap<String, hashBlock>();
    public BlockServerHandler() {
   	
    }

	@Override
	public response storeBlock(hashBlock hashblock)
		throws org.apache.thrift.TException
	{
		System.out.println("storeBlock()");
		System.out.println("  got block w/ hash " + hashblock.hash);

		byte[] buf = hashblock.block.array();

		System.out.println("  block is of length " + buf.length);

		//ByteBuffer bytebuf = ByteBuffer.wrap(buf);
		hashBlock block = new hashBlock();
		block.block = hashblock.block;
		block.hash = hashblock.hash;
		hashblocks.put(hashblock.hash, hashblock);
		response r = new response();
		r.message = responseType.OK;
		return r;
	}

	@Override
	public hashBlock getBlock(String hash)
		throws org.apache.thrift.TException {
		System.out.println("getBlock()");
		if (hashblocks.containsKey(hash)) {
			System.out.println("  got block w/ hash " + hash);
			System.out.println("  block is of length " + hashblocks.get(hash).block.array().length);
			return hashblocks.get(hash);
		}
		else
			return null;
	}

	@Override
	public response deleteBlock(String hash)
		throws org.apache.thrift.TException {
		System.out.println("deleteBlock()");
		response r = new response();
		r.message = responseType.ERROR;
		for (String key : hashblocks.keySet()) {
			if (key.equals(hash)) {
				hashblocks.remove(key);
				r.message = responseType.OK;
				break;
			}
		}
		return r;
	}
	@Override
	public boolean hasBlock(String hash)
	    throws org.apache.thrift.TException {
	    System.out.println("hasBlock()");
		return hashblocks.containsKey(hash);
	}

}
