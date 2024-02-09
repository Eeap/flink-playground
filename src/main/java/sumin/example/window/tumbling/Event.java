package sumin.example.window.tumbling;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class Event implements Serializable {
    public Long eventID;
    public String eventName;
    public String eventTime;
    public String eventMessage;
    public Long getTime() throws ParseException {
        SimpleDateFormat setTime = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS Z");
        setTime.setTimeZone(TimeZone.getTimeZone("Asia/Seoul"));
        return setTime.parse(this.eventTime).toInstant().toEpochMilli();
    }
    public String toString() {
        return "Event ID: " + this.eventID +
                " Event Name: " + this.eventName +
                " Event Time: " + this.eventTime +
                " Event Message: " + this.eventMessage;
    }
}
