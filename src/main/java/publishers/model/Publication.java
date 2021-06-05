package publishers.model;

import lombok.*;

import java.time.LocalDate;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class Publication {
    private Long stationId;
    private String city;
    private Long temperature;
    private Double rainChance;
    private Long windSpeed;
    private LocalDate date;

}

