package PrefixSpan;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by prm14 on 2015/12/16.
 */
public class PrefixSpan_input implements Serializable {
    private String user;
    private String item;
    private Date behaviorTime;
    public PrefixSpan_input setUser(String s){
        user=s;
        return this;
    }
    public PrefixSpan_input setItem(String s){
        item=s;
        return this;
    }
    public PrefixSpan_input setBehaviorTime(Date s){
        behaviorTime=s;
        return this;
    }
    public String getUser(){
        return user;
    }
    public String getItem(){
        return item;
    }
    public Date getBehaviorTime(){
        return behaviorTime;
    }
}
