from fastapi import FastAPI, HTTPException

from methods import load_data, get_weather_method, get_weather_sum_method

app = FastAPI()


@app.on_event("startup")
async def startup_event():
    load_data()


@app.get('/weather/data')
async def get_weather(lat: float, lon: float):
    try:
        return get_weather_method(lat=lat, lon=lon)
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to get weather data")


@app.get('/weather/summarize')
async def get_weather_summarize(lat: float, lon: float):
    try:
        return get_weather_sum_method(lat=lat, lon=lon)
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to get weather summarize")

