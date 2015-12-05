package IBCF;

import java.io.Serializable;

/**
 * Created by prm14 on 2015/12/5.
 */

public class IBCF_input implements Serializable {
    private String user;
    private String item;
    public IBCF_input setUser(String s){
        user=s;
        return this;
    }
    public IBCF_input setItem(String s){
        item=s;
        return this;
    }
    public String getUser(){
        return user;
    }
    public String getItem(){
        return item;
    }
}