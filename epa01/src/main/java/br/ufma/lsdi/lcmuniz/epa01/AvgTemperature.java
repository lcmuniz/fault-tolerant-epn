package br.ufma.lsdi.lcmuniz.epa01;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Date;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class AvgTemperature {
    private double value;
    private Date timestamp;
}
