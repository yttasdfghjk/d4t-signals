import os
from dotenv import load_dotenv
from fastapi import FastAPI, status#,HTTPExeption, Depends
from pydantic import BaseModel
from apscheduler.schedulers.background import BackgroundScheduler
from contextlib import asynccontextmanager
import uvicorn
import pandas as pd
import numpy as np
from datetime import datetime, timezone
import utils.screener as screener, utils.technicaltools as technicaltools, utils.telegramtools as telegramtools

from utils.trashtools import clean_trash
import logging



#global counter 
counter = 0
######################


load_dotenv(override=True)
telegram_api_token = os.getenv("TELEGRAM_API_TOKEN")
telegram_groupchatID = os.getenv("TELEGRAM_GROUPCHAT_ID")
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")

tickers = screener.get_static_watchlist()


def signals_job(tickers, timeframe):
    """
    gets OHLC Dataframes for all tickers in a timeframe
    todo: dask parallel, db--> only on init get all data
    """
    #dfPriceList = [] # tempList of DFs --> 1 signals message per timeframe, ODER write2DB? 
    global counter
    local_now = datetime.now()
    utc_now = local_now.astimezone(timezone.utc)
    utc_time = utc_now.strftime("%d:%m:%Y-%H:%M:%S")
    time_info = "UTC Time: " + utc_time
    message = time_info
    total_signals = 0
    total_warnings = 0

    for t in tickers:
        df = screener.get_ohlc(t, timeframe, limit=1000)
        df = technicaltools.populate_features(df, timeframe) 
        message, signals_count, warnings_count = telegramtools.aggregate_message(message, t, timeframe, df)
        total_signals += signals_count
        total_warnings += warnings_count

    if total_signals > 0 or total_warnings > 0:
        telegramtools.send2telegram(message, telegram_api_token, telegram_groupchatID)
    else:
        pass
    counter += 1
    if timeframe == "1h":
        clean_trash()


@asynccontextmanager
async def lifespan(_: FastAPI):
    logging.basicConfig()
    logging.getLogger('apscheduler').setLevel(logging.INFO)

    print('Starting up...')
    start_time = datetime.now()
    start_ts = start_time.strftime("%d:%m:%Y-%H:%M:%S")
    print("Start time: "+ start_ts)

    scheduler = BackgroundScheduler()
    scheduler.add_job(id="1d", func=signals_job,args=[tickers,"1d"], trigger='cron', day='*', hour='0', jitter=10, max_instances=10)
    scheduler.add_job(id="4h", func=signals_job, args=[tickers,"4h"], trigger='cron', hour='*/4', jitter=10, max_instances=10) 
    scheduler.add_job(id="1h", func=signals_job, args=[tickers,"1h"], trigger='cron', hour='*', jitter=10, max_instances=10)
    scheduler.add_job(id="15m", func=signals_job, args=[tickers,"15m"], trigger='cron', minute='*/15', jitter=10, max_instances=10)
    #scheduler.add_job(id="5m", func=signals_job, args=[tickers,"5m"], trigger='cron', minute='*/5')
    #scheduler.add_job(id="1m", func=signals_job, args=[tickers,"1m"], trigger='cron', minute='*/1')
    #scheduler.add_job(id="0", func=clean_trash, trigger='cron', minute='*/5', jitter=10)

    scheduler.start()
    yield
    
    print('Shutting down...')
    stop_time = datetime.now()
    stop_ts = stop_time.strftime("%d:%m:%Y-%H:%M:%S")
    print("Stop time: "+ stop_ts)

    scheduler.shutdown(wait=False)


app = FastAPI(
    title="d4t",
    description="Test API",
    version="v1",
    #docs_url="/", ### beißt sich mit dashboard route
    lifespan=lifespan,
    #prod no docs pls
    docs_url=None,
    redoc_url=None
)


class HealthCheck(BaseModel):
    """Response model to validate and return when performing a health check."""
    
    status: str = "OK"

@app.get(
    "/health",
    tags=["healthcheck"],
    summary="Perform a Health Check",
    response_description="Return HTTP Status Code 200 (OK)",
    status_code=status.HTTP_200_OK,
    response_model=HealthCheck,
)
async def get_health() -> HealthCheck:
    """
    ## Perform a Health Check
    Endpoint to perform a healthcheck on. This endpoint can primarily be used Docker
    to ensure a robust container orchestration and management is in place. Other
    services which rely on proper functioning of the API service will not deploy if this
    endpoint returns any other HTTP status code except 200 (OK).
    Returns:
        HealthCheck: Returns a JSON response with the health status
    """
    return HealthCheck(status="OK")

@app.get("/")
async def index():
   return {"message": "Hello World"}


if __name__ == "__main__":
    #uvicorn.run("main:app")
    #prod
    uvicorn.run("main:app", host=HOST, port=PORT)
