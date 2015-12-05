package IBCF;

import java.io.Serializable;

/**
 * Created by prm14 on 2015/12/5.
 */
public class IBCF_output implements Serializable {
    private String item;
    private String itemList;
    public IBCF_output setItem(String s){
        item=s;
        return this;
    }
    public IBCF_output setItemList(String s){
        itemList=s;
        return this;
    }
    public String getItem(){
        return item;
    }
    public String getItemList(){
        return itemList;
    }
}