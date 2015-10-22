package datrain;

import java.util.ArrayList;

/**
 * Created by prm14 on 2015/10/22.
 */
public class ItemInfo {
    String item2;
    long count;
    ArrayList<String> info;
    public ItemInfo(String i2,long n,ArrayList<String> in){
        item2=i2;
        count=n;
        info=new ArrayList<String>(in);
    }
    public ItemInfo(ItemInfo ii){
        item2=ii.item2;
        count=ii.count;
        info=new ArrayList<String>(ii.info);
    }
}
