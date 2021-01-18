package ordo;

import java.io.Serializable;

public class Info implements Serializable {
    private String message;

    public Info(String mess) {
        message = mess;
    }

    public void setMessage(String mess) {
        message = mess;
    }

    public String getMessage() {
        return message;
    }
}
