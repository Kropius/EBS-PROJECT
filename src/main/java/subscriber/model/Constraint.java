package subscriber.model;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class Constraint {
    private String key;
    private String operator;
    private String value;
}
