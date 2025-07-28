package org.example;

import com.opencsv.CSVWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;

public class Main {

    // --- НАСТРОЙКИ ПОДКЛЮЧЕНИЯ ---
    // Путь к ненормализованной базе данных SQLite
    private static final String SQLITE_DB_URL = "jdbc:sqlite:denormalized_data.sqlite";

    // Параметры для подключения к нормализованной базе данных PostgreSQL
    private static final String POSTGRES_DB_URL = "jdbc:postgresql://localhost:5432/airline_db";
    private static final String POSTGRES_USER = "postgres"; // Ваш пользователь
    private static final String POSTGRES_PASSWORD = "1234"; // ВАШ ПАРОЛЬ

    public static void main(String[] args) {
        System.out.println("Процесс запущен...");

        try {
            // Шаг 1: Перенос данных из SQLite в PostgreSQL (импорт)
            transferDataFromSqliteToPostgres();

            // Шаг 2: Экспорт отчета из PostgreSQL в CSV-файл
            exportFlightReportToCsv("flight_report.csv");

            System.out.println("Процесс успешно завершен!");

        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Метод для переноса данных из ненормализованной БД SQLite в нормализованную PostgreSQL.
     * Реализует требование "прием и передача данных без модификации".
     */
    public static void transferDataFromSqliteToPostgres() throws SQLException {
        System.out.println("Начинаю перенос данных из SQLite в PostgreSQL...");

        try (Connection sqliteConn = DriverManager.getConnection(SQLITE_DB_URL);
             Connection postgresConn = DriverManager.getConnection(POSTGRES_DB_URL, POSTGRES_USER, POSTGRES_PASSWORD);
             Statement stmt = sqliteConn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM denormalized_flights")) {

            postgresConn.setAutoCommit(false);

            // Запросы остались те же
            String insertAirportSQL = "INSERT INTO airports (airport_code, airport_name, city, country) VALUES (?, ?, ?, ?) ON CONFLICT (airport_code) DO NOTHING";
            String insertAirlineSQL = "INSERT INTO airlines (airline_name) VALUES (?) ON CONFLICT (airline_name) DO NOTHING RETURNING airline_id";
            String insertAircraftSQL = "INSERT INTO aircrafts (model, capacity, airline_id) VALUES (?, ?, ?) ON CONFLICT DO NOTHING RETURNING aircraft_id";
            String insertPassengerSQL = "INSERT INTO passengers (first_name, last_name, passport_number) VALUES (?, ?, ?) ON CONFLICT (passport_number) DO NOTHING RETURNING passenger_id";
            String insertFlightSQL = "INSERT INTO flights (flight_number, departure_airport_code, arrival_airport_code, departure_time, arrival_time, aircraft_id) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING RETURNING flight_id";
            String insertTicketSQL = "INSERT INTO tickets (flight_id, passenger_id, seat_number) VALUES (?, ?, ?) ON CONFLICT (flight_id, passenger_id) DO NOTHING";

            try (PreparedStatement psAirport = postgresConn.prepareStatement(insertAirportSQL);
                 PreparedStatement psAirline = postgresConn.prepareStatement(insertAirlineSQL);
                 PreparedStatement psAircraft = postgresConn.prepareStatement(insertAircraftSQL);
                 PreparedStatement psPassenger = postgresConn.prepareStatement(insertPassengerSQL);
                 PreparedStatement psFlight = postgresConn.prepareStatement(insertFlightSQL);
                 PreparedStatement psTicket = postgresConn.prepareStatement(insertTicketSQL)) {

                while (rs.next()) {
                    // 1. Вставка аэропортов (здесь не нужен RETURNING, executeUpdate подходит)
                    psAirport.setString(1, rs.getString("departure_airport_code"));
                    psAirport.setString(2, rs.getString("departure_airport_name"));
                    psAirport.setString(3, rs.getString("departure_city"));
                    psAirport.setString(4, rs.getString("departure_country"));
                    psAirport.executeUpdate();

                    psAirport.setString(1, rs.getString("arrival_airport_code"));
                    psAirport.setString(2, rs.getString("arrival_airport_name"));
                    psAirport.setString(3, rs.getString("arrival_city"));
                    psAirport.setString(4, rs.getString("arrival_country"));
                    psAirport.executeUpdate();

                    // --- ИЗМЕНЕНИЯ ЗДЕСЬ ---

                    // 2. Вставка авиакомпании и получение ее ID
                    psAirline.setString(1, rs.getString("airline_name"));
                    boolean hasAirlineResult = psAirline.execute(); // Используем execute()
                    ResultSet airlineRs = hasAirlineResult ? psAirline.getResultSet() : null;
                    int airlineId = getGeneratedId(airlineRs, postgresConn, "SELECT airline_id FROM airlines WHERE airline_name = ?", rs.getString("airline_name"));

                    // 3. Вставка самолета и получение его ID
                    psAircraft.setString(1, rs.getString("aircraft_model"));
                    psAircraft.setInt(2, rs.getInt("aircraft_capacity"));
                    psAircraft.setInt(3, airlineId);
                    boolean hasAircraftResult = psAircraft.execute(); // Используем execute()
                    ResultSet aircraftRs = hasAircraftResult ? psAircraft.getResultSet() : null;
                    // Для самолета нужен более сложный запрос для поиска, т.к. нет UNIQUE约束
                    int aircraftId = getGeneratedId(aircraftRs, postgresConn, "SELECT aircraft_id FROM aircrafts WHERE model = ? AND airline_id = ? AND capacity = ?", rs.getString("aircraft_model"), airlineId, rs.getInt("aircraft_capacity"));

                    // 4. Вставка пассажира и получение его ID
                    psPassenger.setString(1, rs.getString("passenger_first_name"));
                    psPassenger.setString(2, rs.getString("passenger_last_name"));
                    psPassenger.setString(3, rs.getString("passenger_passport_number"));
                    boolean hasPassengerResult = psPassenger.execute(); // Используем execute()
                    ResultSet passengerRs = hasPassengerResult ? psPassenger.getResultSet() : null;
                    int passengerId = getGeneratedId(passengerRs, postgresConn, "SELECT passenger_id FROM passengers WHERE passport_number = ?", rs.getString("passenger_passport_number"));

                    // 5. Вставка рейса и получение его ID
                    psFlight.setString(1, rs.getString("flight_number"));
                    psFlight.setString(2, rs.getString("departure_airport_code"));
                    psFlight.setString(3, rs.getString("arrival_airport_code"));
                    psFlight.setTimestamp(4, Timestamp.valueOf(rs.getString("departure_time")));
                    psFlight.setTimestamp(5, Timestamp.valueOf(rs.getString("arrival_time")));
                    psFlight.setInt(6, aircraftId);
                    boolean hasFlightResult = psFlight.execute(); // Используем execute()
                    ResultSet flightRs = hasFlightResult ? psFlight.getResultSet() : null;
                    // Для рейса тоже нужен более сложный запрос
                    int flightId = getGeneratedId(flightRs, postgresConn, "SELECT flight_id FROM flights WHERE flight_number = ? AND departure_time = ? AND aircraft_id = ?", rs.getString("flight_number"), Timestamp.valueOf(rs.getString("departure_time")), aircraftId);

                    // 6. Вставка билета
                    psTicket.setInt(1, flightId);
                    psTicket.setInt(2, passengerId);
                    psTicket.setString(3, rs.getString("seat_number"));
                    psTicket.executeUpdate();
                }
                postgresConn.commit();
                System.out.println("Данные успешно перенесены.");
            }
        }
    }

    /**
     * Вспомогательный метод для получения ID вставленной записи.
     * Если ON CONFLICT сработал, RETURNING не вернет ID, поэтому делаем дополнительный SELECT.
     * Теперь он корректно обрабатывает случай, когда ResultSet изначально null.
     */
    private static int getGeneratedId(ResultSet rs, Connection conn, String selectQuery, Object... params) throws SQLException {
        // Проверяем, вернул ли RETURNING результат
        if (rs != null && rs.next()) {
            return rs.getInt(1);
        } else {
            // Если RETURNING был пуст, ищем ID существующей записи
            try (PreparedStatement ps = conn.prepareStatement(selectQuery)) {
                for (int i = 0; i < params.length; i++) {
                    ps.setObject(i + 1, params[i]);
                }
                ResultSet selectRs = ps.executeQuery();
                if (selectRs.next()) {
                    return selectRs.getInt(1);
                }
            }
        }
        // Если ID не найден ни через RETURNING, ни через SELECT, это исключительная ситуация
        throw new SQLException("Не удалось получить или найти ID для запроса: " + selectQuery);
    }


    /**
     * Метод для экспорта отчета из PostgreSQL в CSV файл.
     * Реализует требование "экспорт данных в документ электронных таблиц".
     * Генерация CSV - это и есть "расстановка данных", выполняемая приложением.
     */
    public static void exportFlightReportToCsv(String filePath) throws SQLException, IOException {
        System.out.println("Начинаю экспорт отчета в " + filePath + "...");
        String query = "SELECT " +
                "f.flight_number, " +
                "dep.airport_name as departure_airport, " +
                "arr.airport_name as arrival_airport, " +
                "f.departure_time, " +
                "al.airline_name, " +
                "ac.model as aircraft_model, " +
                "COUNT(t.ticket_id) as passengers_on_board " +
                "FROM flights f " +
                "JOIN airports dep ON f.departure_airport_code = dep.airport_code " +
                "JOIN airports arr ON f.arrival_airport_code = arr.airport_code " +
                "JOIN aircrafts ac ON f.aircraft_id = ac.aircraft_id " +
                "JOIN airlines al ON ac.airline_id = al.airline_id " +
                "LEFT JOIN tickets t ON f.flight_id = t.flight_id " +
                "GROUP BY f.flight_number, dep.airport_name, arr.airport_name, f.departure_time, al.airline_name, ac.model " +
                "ORDER BY f.departure_time;";

        try (Connection conn = DriverManager.getConnection(POSTGRES_DB_URL, POSTGRES_USER, POSTGRES_PASSWORD);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query);
             CSVWriter writer = new CSVWriter(new FileWriter(filePath))) {

            // --- ИСПРАВЛЕННЫЙ БЛОК ---
            // Получаем метаданные для записи заголовков
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            // Создаем массив для хранения заголовков
            String[] headers = new String[columnCount];
            // В цикле получаем имя каждой колонки. Индексы в JDBC начинаются с 1.
            for (int i = 1; i <= columnCount; i++) {
                // Используем getColumnLabel, чтобы получить псевдонимы (aliases) типа "AS departure_airport"
                headers[i - 1] = metaData.getColumnLabel(i);
            }
            // Записываем заголовок CSV файла
            writer.writeNext(headers);
            // --- КОНЕЦ ИСПРАВЛЕННОГО БЛОКА ---

            // Записываем все строки данных (без повторной записи заголовков)
            writer.writeAll(rs, false);

            System.out.println("Отчет успешно сохранен.");
        }
    }
}