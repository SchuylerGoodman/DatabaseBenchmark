/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package dbtesting;

import redis.clients.jedis.Jedis;

/**
 *
 * @author Owner
 */
public class RedisTest extends Test
{

    private Jedis jedis;
    
    @Override
    public void init() throws Exception 
    {
        jedis=new Jedis("localhost");
        jedis.connect();
    }

    @Override
    public long createCollection(String collectionName) throws Exception
    {
        tick();
        jedis.set(collectionName, "created");
        return tock();
    }

    @Override
    public long createDocument(String collectionName, int key, byte[] data0, byte[] data1, byte[] data2) throws Exception
    {
    	collectionName=collectionName.concat(Integer.toString(key));
    	byte[] list=new byte[data0.length+data1.length+data2.length];
        for(int i=0; i<list.length; i++)
        {
            if(i<data0.length)
                list[i]=data0[i];
            else if(i<(data1.length+data0.length))
                list[i]=data1[i-data0.length];
            else
                list[i]=data2[i-(data1.length+data0.length)];
        }
        tick();
    	jedis.set(collectionName.getBytes(), list);
        return tock();
    }

    @Override
    public long readDocument(String collectionName, int key) throws Exception
    {
        collectionName=collectionName.concat(Integer.toString(key));
        tick();
        jedis.get(collectionName);
        return tock();
    }

    @Override
    public long readValue(String collectionName, int key, int value) throws Exception
    {
        tick();
        jedis.get(collectionName.concat(Integer.toString(key)));
        return tock();
    }

    @Override
    public long updateDocument(String collectionName, int key, byte[] data0, byte[] data1, byte[] data2) throws Exception
    {
    	collectionName=collectionName.concat(Integer.toString(key));
    	byte[] list=new byte[data0.length+data1.length+data2.length];
        for(int i=0; i<list.length; i++)
        {
            if(i<data0.length)
                list[i]=data0[i];
            else if(i<(data1.length+data0.length))
                list[i]=data1[i-data0.length];
            else
                list[i]=data2[i-(data1.length+data0.length)];
        }
        tick();
    	jedis.set(collectionName.getBytes(), list);
        return tock();
    }

    @Override
    public long updateValue(String collectionName, int key, byte[] data) throws Exception
    {
        tick();
    	jedis.set(collectionName.concat(Integer.toString(key)).getBytes(), data);
        return tock();
    }

    @Override
    public long deleteDocument(String collectionName, int key) throws Exception
    {
        tick();
        jedis.expire(collectionName.concat(Integer.toString(key)).getBytes(), 1);
        return tock();
    }

    @Override
    public long deleteCollection(String collectionName) throws Exception
    {
        tick();
        jedis.expire(collectionName.getBytes(), 1);
        return tock();
    }

    @Override
    public void cleanup() throws Exception 
    {
        jedis.disconnect();
    }
    
}
