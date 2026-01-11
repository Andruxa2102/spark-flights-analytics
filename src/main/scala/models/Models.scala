package models

import java.time.LocalDate

object Models {

  case class Flight(
                     weekDay:              String,
                     airline:              String,
                     originAirport:        String,
                     destinationAirport:   String,
                     departureDelay:       String,
                     arrivalDelay:         String,
                     airSystemDelay:       String,
                     securityDelay:        String,
                     airlineDelay:         String,
                     lateAircraftDelay:    String,
                     weatherDelay:         String
                    )


  case class FlightWithDate(
                           date:                 LocalDate,
                           weekDay:              String,
                           airline:              String,
                           originAirport:        String,
                           destinationAirport:   String,
                           departureDelay:       String,
                           arrivalDelay:         String,
                           airSystemDelay:       String,
                           securityDelay:        String,
                           airlineDelay:         String,
                           lateAircraftDelay:    String,
                           weatherDelay:         String
                         )
}
