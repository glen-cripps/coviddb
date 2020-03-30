import schedule
import time
import os
import arrow

# Quick logging of the current date time
now_ts = arrow.now().format('YYYYMMDDHHmmss')
print("Master scheduler program running at " + now_ts)

# this procedure (job) simply executes the python programs I want running daily.  It is called in a neverending loop below
def job():
    now_ts = arrow.now().format('YYYYMMDDHHmmss')
    print("job starting at ..." + now_ts)

#    now_ts = arrow.now().format('YYYYMMDDHHmmss')
#    os.system('python /home/user/Glens_work/000_jupyterminator.py > /home/user/Glens_work/000_jupyterminator.' + now_ts + '.log 2>&1')

    now_ts = arrow.now().format('YYYYMMDDHHmmss')
    os.system('python /home/user/coviddb/twitter/100_read_tweets.py > /home/user/coviddb/twitter/100_read_tweets.' + now_ts + '.log 2>&1')
    
    now_ts = arrow.now().format('YYYYMMDDHHmmss')
    os.system('python /home/user/coviddb/twitter/200_tweets_to_parquet.py > /home/user/coviddb/twitter/200_tweets_to_parquet.' + now_ts + '.log 2>&1')
    
    now_ts = arrow.now().format('YYYYMMDDHHmmss')
    print("job ending at ..." + now_ts)

# this procedure (job) simply executes the python programs I want running daily.  It is called in a neverending loop below
def yahoo_job():
    now_ts = arrow.now().format('YYYYMMDDHHmmss')
    print("yahoo job starting at ..." + now_ts)

    now_ts = arrow.now().format('YYYYMMDDHHmmss')
    os.system('python /home/user/Glens_work/100_yahoo_nasdaq_news_pull.py > /home/user/Glens_work/100_yahoo_nasdaq_news_pull.' + now_ts + '.log 2>&1 &')

    now_ts = arrow.now().format('YYYYMMDDHHmmss')
    os.system('python /home/user/Glens_work/100_yahoo_nyse_news_pull.py > /home/user/Glens_work/100_yahoo_nyse_news_pull.' + now_ts + '.log 2>&1 &')

    now_ts = arrow.now().format('YYYYMMDDHHmmss')
    os.system('python /home/user/Glens_work/100_yahoo_amex_news_pull.py > /home/user/Glens_work/100_yahoo_amex_news_pull.' + now_ts + '.log 2>&1 &')

    now_ts = arrow.now().format('YYYYMMDDHHmmss')
    print("job ending at ..." + now_ts)

# This statement adds the "job" procedure to a scheduling database, like cron 
schedule.every().day.at("01:00").do(job)

#schedule.every().monday.at("22:00").do(yahoo_job)
#schedule.every().wednesday.at("22:00").do(yahoo_job)
#schedule.every().friday.at("22:00").do(yahoo_job)
#schedule.every().tuesday.at("19:10").do(yahoo_job)

#schedule.every().friday.at("00:00").do(job)
# This is the forever loop which runs and when the clock strikes midnight GMT (7pm EST here) it runs the job above
while True:
    schedule.run_pending()
    time.sleep(1)

