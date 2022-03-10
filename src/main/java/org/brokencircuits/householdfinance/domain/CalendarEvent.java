package org.brokencircuits.householdfinance.domain;

import java.time.LocalDate;
import lombok.Value;

@Value
public class CalendarEvent {

  LocalDate date;
  String title;
}
