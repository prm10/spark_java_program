package PrefixSpanMethod;

import java.io.Serializable;

/**
 * Created by prm14 on 2015/12/16.
 */
public class PrefixSpan_output implements Serializable {
    private String pattern;
    private String times;
    public PrefixSpan_output setPattern(String s){
        pattern=s;
        return this;
    }
    public PrefixSpan_output setTimes(Long s){
        times=s.toString();
        return this;
    }

    public String getPattern(){
        return pattern;
    }
    public String getTimes(){
        return times;
    }
}
