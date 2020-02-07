import tweepy
from tweepy import Stream
from tweepy.streaming import StreamListener 
from tweepy import OAuthHandler
import json
import time
import sys

consumer_key = "dRjZHiNvJb2fxRzIbHAapjh1k"
consumer_secret = "wNuiplvZFOMEBg64ooSImTWk8mDZLAtf62ZuXhrUTRPEa9j4mU"
access_token = "1221172259018031105-0ppIL3uaH9HXC3fbZFjJp7uQxhBSSU"
access_token_secret = "BvBfM0XrQ4fQvTyrgvHxxxCg796nKq1RJx4hCLSWxMBtI"
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth,wait_on_rate_limit=True)

#arguments
numTweets = int(sys.argv[1]) #num of tweets 
dirName = str(sys.argv[2]) #data path
tweetcount = 0


def parse(cls, api, raw):
    status = cls.first_parse(api, raw)
    setattr(status, 'json', json.dumps(raw))
    return status
 
# Status() is the data model for a tweet
tweepy.models.Status.first_parse = tweepy.models.Status.parse
tweepy.models.Status.parse = parse
class MyListener(StreamListener):
 
    def on_data(self, data):
        global tweetcount
        #Stop when the number of requested Tweets is reached
        if tweetcount >= numTweets and numTweets != 0:
            print('Reached ' + str(numTweets) +  ' tweets. Stopped')
            return False
        try:
            outputPath = dirName + '/twitterData.json'
            with open(outputPath, 'a') as f:
                f.write(data)
                tweetcount += 1
                # Stop if data size > 5GB
                if (f.tell() >= 1024 * 1024 * 1024 * 5):
                    print('Reached 5GB of data. Stopped')
                    return False
                return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        if (status == 420):
            print("Error code 420")
            return False
 
#Set the hashtag to be searched
twitter_stream = Stream(auth, MyListener())
twitter_stream.filter(track=['impeachment'], is_async=True)