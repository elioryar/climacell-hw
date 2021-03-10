import os

import uvicorn
from fastapi import FastAPI, HTTPException

from methods import load_data, get_weather_method, get_weather_sum_method

app = FastAPI()


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


if __name__ == "__main__":
    load_data()
    uvicorn.run(app, host="0.0.0.0", port=os.environ.get('PORT'))
