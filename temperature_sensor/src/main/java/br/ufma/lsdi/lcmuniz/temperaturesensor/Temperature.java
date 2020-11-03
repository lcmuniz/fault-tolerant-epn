package br.ufma.lsdi.lcmuniz.temperaturesensor;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Date;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class Temperature {
    private double value;
    private Date timestamp;
}
