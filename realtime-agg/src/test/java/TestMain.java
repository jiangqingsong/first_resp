import com.rock.analyse.util.RockConstants;
import org.joda.time.DateTime;

import java.util.Date;

public class TestMain {
    public static void main(String[] args) {

         long minInterval = 13*60*1000;
        long hourInterval = 24*60*60*1000;

         long timestamp = System.currentTimeMillis();

        long start = timestamp - (timestamp % (hourInterval));

        DateTime cTIme =  new DateTime(start);

        DateTime startTIme =  new DateTime(start);

        DateTime endTime =  new DateTime(start + hourInterval);

        System.out.println("当前时间  " + cTIme.toString(RockConstants.DATE_TIME_PATTERN));
        System.out.println("窗口开始时间  " + startTIme.toString(RockConstants.DATE_TIME_PATTERN));
        System.out.println("窗口结束时间  " + endTime.toString(RockConstants.DATE_TIME_PATTERN));

    }
}
