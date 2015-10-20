package experiment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by prm14 on 2015/10/20.
 */
public class e1 {

    public static void main(String[] args){
        String s="12,345,678,890";
        Pattern SPACE = Pattern.compile(",");
//        String[] b=qie.split(a);
//        StringBuffer c=new StringBuffer();
//        for(int i=1;i<b.length;i++){
//            c.append(b[i]);
//            c.append(',');
//        }
        List<String> s2= Arrays.asList(SPACE.split(s));
        String user_id=s2.get(0);
        ArrayList<String> s3=new ArrayList<String>(s2);
        s3.remove(0);
        String d=s2.toString();
        System.out.println(d);
    }
}
