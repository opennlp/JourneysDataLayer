from dailymotionfetch import dailymotionfetchdriver
from flickr import flickrfetch
from git import gitfetch
from helpx import helpxfetch
from imgur import imgurfetch
from jira import jirafetch
from libraryio import libraryiofetch
from news import newsfetch
from pixabayapi import pixabayfetch
from reddit import redditfetch
from twitter import twitterfetch
from youtube import youtubefetch

import schedule
import time


def init_all_fetch():
    dailymotionfetchdriver.dailymotion_fetch_and_store()
    flickrfetch.flickr_fetch_and_persist()
    gitfetch.git_fetch_and_persist()
    imgurfetch.imgur_fetch_and_persist()
    jirafetch.jira_fetch_and_persist()
    libraryiofetch.libraryio_fetch_and_persist()
    newsfetch.news_fetch_and_persist()
    pixabayfetch.pixabay_fetch_and_persist()
    redditfetch.reddit_fetch_and_persist()
    twitterfetch.twitter_search_and_persist()
    youtubefetch.youtube_fetch_and_persist()
    helpxfetch.helpx_fetch_schedule()


if __name__ == "__main__":
    schedule.every(10).seconds.do(init_all_fetch)
    while True:
        schedule.run_pending()
        time.sleep(10)
