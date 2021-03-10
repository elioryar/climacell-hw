from db_handler import db_connection


def load_data():
    # check if need commit
    print("started")
    db_connection.execute_query("DROP TABLE IF EXISTS forecast")
    db_connection.execute_query("CREATE TABLE forecast(Longitude FLOAT ,"
                                "Latitude FLOAT,"
                                "forecast_time DATETIME,"
                                "Temperature_Celsius FLOAT, "
                                "Precipitation_Rate FLOAT)")

    db_connection.execute_query(
        "LOAD DATA LOCAL INFILE './csv_files/file1.csv' INTO TABLE climacell_db.forecast FIELDS TERMINATED BY ',' " \
        "LINES TERMINATED BY '\n' IGNORE 1 LINES (Longitude, Latitude, forecast_time, Temperature_Celsius, Precipitation_Rate)")
    db_connection.execute_query(
        "LOAD DATA LOCAL INFILE './csv_files/file2.csv' INTO TABLE climacell_db.forecast FIELDS TERMINATED BY ',' " \
        "LINES TERMINATED BY '\n' IGNORE 1 LINES (Longitude, Latitude, forecast_time, Temperature_Celsius, Precipitation_Rate)")
    db_connection.execute_query(
        "LOAD DATA LOCAL INFILE './csv_files/file3.csv' INTO TABLE climacell_db.forecast FIELDS TERMINATED BY ',' " \
        "LINES TERMINATED BY '\n' IGNORE 1 LINES (Longitude, Latitude, forecast_time, Temperature_Celsius, Precipitation_Rate)")


def get_weather_method(lat: float, lon: float):
    res = []
    result = db_connection.fetch_query(
        f'SELECT forecast_time, Temperature_Celsius, Precipitation_Rate FROM forecast WHERE Longitude= "{lon}" AND Latitude ="{lat}"')
    for row in result:
        res.append({"forecastTime": row[0].isoformat(), "Temperature": row[1], "Precipitation": row[2]})
    return res


def get_weather_sum_method(lat: float, lon: float):
    result = db_connection.fetch_query(
        f'SELECT MAX(Temperature_Celsius) FROM forecast WHERE Longitude= "{lon}" AND Latitude ="{lat}"')
    res = {"max": {"Temperature": result[0][0]}}
    result = db_connection.fetch_query(
        f'SELECT MAX(Precipitation_Rate) FROM forecast WHERE Longitude= "{lon}" AND Latitude ="{lat}"')
    res["max"].update({"Precipitation": result[0][0]})
    result = db_connection.fetch_query(
        f'SELECT MIN(Temperature_Celsius) FROM forecast WHERE Longitude= "{lon}" AND Latitude ="{lat}"')
    res.update({"min": {"Temperature": result[0][0]}})
    result = db_connection.fetch_query(
        f'SELECT MIN(Precipitation_Rate) FROM forecast WHERE Longitude= "{lon}" AND Latitude ="{lat}"')
    res["min"].update({"Precipitation": result[0][0]})
    result = db_connection.fetch_query(
        f'SELECT AVG(Temperature_Celsius) FROM forecast WHERE Longitude= "{lon}" AND Latitude ="{lat}"')
    res.update({"avg": {"Temperature": round(result[0][0], 2)}})
    result = db_connection.fetch_query(
        f'SELECT AVG(Precipitation_Rate) FROM forecast WHERE Longitude= "{lon}" AND Latitude ="{lat}"')
    res["avg"].update({"Precipitation": round(result[0][0], 2)})
    return res
