from helpx.helpxfetch import helpx_fetch_schedule
import schedule
import time

if __name__ == "__main__":
    schedule.every(10).minutes.do(helpx_fetch_schedule)
    while True:
        schedule.run_pending()
        time.sleep(10)
