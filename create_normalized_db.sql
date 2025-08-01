-- Удаляем таблицы, если они существуют, для чистого запуска
DROP TABLE IF EXISTS tickets;
DROP TABLE IF EXISTS flights;
DROP TABLE IF EXISTS passengers;
DROP TABLE IF EXISTS aircrafts;
DROP TABLE IF EXISTS airlines;
DROP TABLE IF EXISTS airports;

-- Создание таблицы аэропортов
CREATE TABLE airports (
                          airport_code CHAR(3) PRIMARY KEY,
                          airport_name VARCHAR(100) NOT NULL,
                          city VARCHAR(50) NOT NULL,
                          country VARCHAR(50) NOT NULL
);

-- Создание таблицы авиакомпаний
CREATE TABLE airlines (
                          airline_id SERIAL PRIMARY KEY,
                          airline_name VARCHAR(100) UNIQUE NOT NULL
);

-- Создание таблицы самолетов (связь 1:M с авиакомпаниями)
CREATE TABLE aircrafts (
                           aircraft_id SERIAL PRIMARY KEY,
                           model VARCHAR(50) NOT NULL,
                           capacity INT NOT NULL,
                           airline_id INT,
                           CONSTRAINT fk_airline
                               FOREIGN KEY(airline_id)
                                   REFERENCES airlines(airline_id)
);

-- Создание таблицы пассажиров
CREATE TABLE passengers (
                            passenger_id SERIAL PRIMARY KEY,
                            first_name VARCHAR(50) NOT NULL,
                            last_name VARCHAR(50) NOT NULL,
                            passport_number VARCHAR(20) UNIQUE NOT NULL
);

-- Создание таблицы рейсов
CREATE TABLE flights (
                         flight_id SERIAL PRIMARY KEY,
                         flight_number VARCHAR(10) NOT NULL,
                         departure_airport_code CHAR(3),
                         arrival_airport_code CHAR(3),
                         departure_time TIMESTAMP NOT NULL,
                         arrival_time TIMESTAMP NOT NULL,
                         aircraft_id INT,
                         CONSTRAINT fk_departure_airport
                             FOREIGN KEY(departure_airport_code)
                                 REFERENCES airports(airport_code),
                         CONSTRAINT fk_arrival_airport
                             FOREIGN KEY(arrival_airport_code)
                                 REFERENCES airports(airport_code),
                         CONSTRAINT fk_aircraft
                             FOREIGN KEY(aircraft_id)
                                 REFERENCES aircrafts(aircraft_id)
);

-- Создание связующей таблицы билетов (M:M между рейсами и пассажирами)
CREATE TABLE tickets (
                         ticket_id SERIAL PRIMARY KEY,
                         flight_id INT,
                         passenger_id INT,
                         seat_number VARCHAR(4),
                         CONSTRAINT fk_flight
                             FOREIGN KEY(flight_id)
                                 REFERENCES flights(flight_id),
                         CONSTRAINT fk_passenger
                             FOREIGN KEY(passenger_id)
                                 REFERENCES passengers(passenger_id),
                         UNIQUE (flight_id, passenger_id) -- Пассажир не может иметь два билета на один рейс
);

-- Сообщение об успешном завершении
SELECT 'Database schema created successfully' as status;