import java.util.HashMap;
import java.util.Map;

public class Stopwatch {
    private static Long startTime;
    private static Long endTime;

    private Stopwatch() {
    }

    static { // Static constructor
        startTime = null;
    }

    public static void setInitialStartTime() {
        startTime = System.currentTimeMillis();
    }

    public static void setEndTime(){
        endTime = System.currentTimeMillis();
    }

    public static Long getTime(){
        return endTime - startTime;
    }

}
