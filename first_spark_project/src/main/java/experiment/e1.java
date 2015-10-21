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
        String[] b=s.split(",");//SPACE.split(s);
        StringBuilder c=new StringBuilder();
        for(int i=1;i<b.length-1;i++){
            c.append(b[i]);
            c.append(',');
        }
        c.append(b[b.length-1]);
//        List<String> s2= Arrays.asList(SPACE.split(s));
//        String user_id=s2.get(0);
//        ArrayList<String> s3=new ArrayList<String>(s2);
//        s3.remove(0);
        System.out.println(b[0]+":"+c.toString());
    }
}
