package com.study.database;

import com.study.database.util.store;

public class main {
    public static void main(String args[]){
        store.createNamespaceAndTable();
        store.storeUserData();
        store.storeWeiboData();
    }
}
