package subscriber.model;

import lombok.*;

import java.util.LinkedList;
import java.util.List;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Subscription {
    private List<Constraint> constraints = new LinkedList<>();
}
