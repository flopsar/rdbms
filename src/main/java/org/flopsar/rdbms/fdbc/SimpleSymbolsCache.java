package org.flopsar.rdbms.fdbc;

import com.flopsar.fdbc.api.SymbolType;

import java.lang.ref.SoftReference;
import java.util.concurrent.ConcurrentHashMap;




public class SimpleSymbolsCache {

    private final ConcurrentHashMap<SymbolType,SoftReference<ConcurrentHashMap<String,Integer>>> cache
            = new ConcurrentHashMap<SymbolType,SoftReference<ConcurrentHashMap<String,Integer>>>();


    public SimpleSymbolsCache(){
        cache.put(SymbolType.SYMBOL_CLASS,new SoftReference<ConcurrentHashMap<String,Integer>>(new ConcurrentHashMap<String,Integer>()));
        cache.put(SymbolType.SYMBOL_THREAD,new SoftReference<ConcurrentHashMap<String,Integer>>(new ConcurrentHashMap<String,Integer>()));
        cache.put(SymbolType.SYMBOL_KV,new SoftReference<ConcurrentHashMap<String,Integer>>(new ConcurrentHashMap<String,Integer>()));
        cache.put(SymbolType.SYMBOL_METHOD,new SoftReference<ConcurrentHashMap<String,Integer>>(new ConcurrentHashMap<String,Integer>()));
    }



    public void put(String symbolName, SymbolType symbolType, int symbolId){
        ConcurrentHashMap<String,Integer> cch = cache.get(symbolType).get();
        if (cch == null){
            cch = new ConcurrentHashMap<String,Integer>();
            cache.putIfAbsent(symbolType,new SoftReference<ConcurrentHashMap<String,Integer>>(cch));
        }
        cch.putIfAbsent(symbolName,symbolId);
    }



    public int getId(String symbolName,SymbolType symbolType){
        ConcurrentHashMap<String,Integer> cch = cache.get(symbolType).get();
        if (cch == null){
            cch = new ConcurrentHashMap<String,Integer>();
            cache.putIfAbsent(symbolType,new SoftReference<ConcurrentHashMap<String,Integer>>(cch));
            return -1;
        }
        Integer id = cch.get(symbolName);
        return (id != null) ? id : -1;
    }



    public void clear(SymbolType symbolType){
        ConcurrentHashMap<String,Integer> cch = cache.get(symbolType).get();
        if (cch != null)
            cch.clear();
    }




}
