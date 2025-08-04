CREATE TABLE denormalized_flights (
                                      flight_number TEXT,
                                      departure_airport_code TEXT,
                                      departure_airport_name TEXT,
                                      departure_city TEXT,
                                      departure_country TEXT,
                                      arrival_airport_code TEXT,
                                      arrival_airport_name TEXT,
                                      arrival_city TEXT,
                                      arrival_country TEXT,
                                      departure_time TEXT,
                                      arrival_time TEXT,
                                      airline_name TEXT,
                                      aircraft_model TEXT,
                                      aircraft_capacity INTEGER,
                                      passenger_first_name TEXT,
                                      passenger_last_name TEXT,
                                      passenger_passport_number TEXT,
                                      seat_number TEXT
);

-- Вставляем тестовые данные
INSERT INTO denormalized_flights VALUES
                                     ('SU101', 'SVO', 'Sheremetyevo', 'Moscow', 'Russia', 'JFK', 'John F. Kennedy', 'New York', 'USA', '2023-10-27 10:00:00', '2023-10-27 12:30:00', 'Aeroflot', 'Boeing 777', 300, 'Ivan', 'Ivanov', '12345678', '12A'),
                                     ('SU101', 'SVO', 'Sheremetyevo', 'Moscow', 'Russia', 'JFK', 'John F. Kennedy', 'New York', 'USA', '2023-10-27 10:00:00', '2023-10-27 12:30:00', 'Aeroflot', 'Boeing 777', 300, 'Maria', 'Petrova', '87654321', '12B'),
                                     ('LH220', 'FRA', 'Frankfurt Airport', 'Frankfurt', 'Germany', 'SVO', 'Sheremetyevo', 'Moscow', 'Russia', '2023-10-28 14:00:00', '2023-10-28 18:00:00', 'Lufthansa', 'Airbus A320', 180, 'John', 'Smith', '99988877', '5C');