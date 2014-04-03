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
public class RedisTest implements Test
{

    private Jedis jedis;
    
    @Override
    public void init() throws Exception 
    {
        jedis=new Jedis("localhost");
        jedis.connect();
    }

    @Override
    public void createCollection(String collectionName) throws Exception 
    {
        jedis.set(collectionName, "created");
    }

    @Override
    public void createDocument(String collectionName, int key, byte[] data0, byte[] data1, byte[] data2) throws Exception 
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
    	jedis.set(collectionName.getBytes(), list);
    }

    @Override
    public void readDocument(String collectionName, int key) throws Exception 
    {
       collectionName=collectionName.concat(Integer.toString(key));
       jedis.get(collectionName);
    }

    @Override
    public void readValue(String collectionName, int key, int value) throws Exception 
    {
        jedis.get(collectionName.concat(Integer.toString(key)));
    }

    @Override
    public void updateDocument(String collectionName, int key, byte[] data0, byte[] data1, byte[] data2) throws Exception 
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
    	jedis.set(collectionName.getBytes(), list);
    }

    @Override
    public void updateValue(String collectionName, int key, byte[] data) throws Exception 
    {
    	jedis.set(collectionName.concat(Integer.toString(key)).getBytes(), data);
    }

    @Override
    public void deleteDocument(String collectionName, int key) throws Exception 
    {
        jedis.expire(collectionName.concat(Integer.toString(key)).getBytes(), 1);
    }

    @Override
    public void deleteCollection(String collectionName) throws Exception 
    {
        jedis.expire(collectionName.getBytes(), 1);
    }

    @Override
    public void cleanup() throws Exception 
    {
        jedis.disconnect();
    }
    
}
