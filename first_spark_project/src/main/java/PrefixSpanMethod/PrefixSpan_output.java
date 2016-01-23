package PrefixSpanMethod;

import java.io.Serializable;

/**
 * Created by prm14 on 2015/12/16.
 */
public class PrefixSpan_output implements Serializable {
    private String pattern;
    private String times;
    private String length;
    public PrefixSpan_output setPattern(String s){
        pattern=s;
        return this;
    }
    public PrefixSpan_output setTimes(Long s){
        times=s.toString();
        return this;
    }
    public PrefixSpan_output setLength(Long s){
        length=s.toString();
        return this;
    }
    public String getPattern(){
        return pattern;
    }
    public String getTimes(){
        return times;
    }
    public String getLength(){
        return length;
    }
}
