import time
import tweepy
from tweepy import StreamListener, Stream
from tweeter_crawler.crawler.authenticator import Authenticator


class TwitterGetter(object):
    def __init__(self, user_ids):
        self._api = tweepy.API(Authenticator.get_authentication_handler())
        self._user_ids = user_ids

    def print_user_details(self, user):
        print "Screen Name:" + user.screen_name
        print "User Name:" + user.name
        print "User Location:" + user.location
        print "User Description:" + user.description
        print "The Number Of Followers:" + str(user.followers_count)
        print "The Number Of Friends:" + str(user.friends_count)
        print "The Number Of Statuses:" + str(user.statuses_count)
        print "User URL:" + str(user.url)


    def get_users_follower_friends_details(self):
        for user_id in self._user_ids:
            friends = self.get_friends_ids(user_id)
            print "Friends:\n\n"
            print "Number of friends: {}".format(len(friends))
            # for friend in friends:
            #     try:
            #         self.print_user_details(self.get_user(friend))
            #     except:
            #         print "Hebrew"

            followers = self.get_followers_ids(user_id)
            print "Followers:\n\n"
            print "Number of followers: {}".format((len(followers)))
            # for follower in followers:
            #     try:
            #         self.print_user_details(self.get_user(follower))
            #     except:
            #         print "Hebrew"

    def get_friends_ids(self, user_id):
        return self._api.friends_ids(user_id)

    def get_followers_ids(self, user_id):
        followers = []
        for page in tweepy.Cursor(self._api.followers_ids, screen_name=user_id).pages():
            time.sleep(10)
            print "current length: {}".format(len(followers))
            followers.extend(page)
        return followers

    def get_user(self, user_id):
        return self._api.get_user(user_id)


class TopicListener(StreamListener):
    def __init__(self, filter_by):
        self._auth = Authenticator.get_authentication_handler()
        self._filter_by = filter_by

    def execute(self, by_geo=False):
        stream = Stream(self._auth, self)
        if by_geo:
            stream.filter(locations=self._filter_by)
        else:
            stream.filter(track=self._filter_by)

    def on_data(self, raw_data):
        print raw_data
        return True

    def on_error(self, status_code):
        print status_code

if __name__ == '__main__':
    twitter_getter = TwitterGetter(['therealguypines'])
    twitter_getter.get_users_follower_friends_details()
    #TopicListener(['Indiana', 'weather']).execute()
    #TopicListener([-86.33,41.63,-86.20,41.74]).execute(True)
