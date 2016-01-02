import time
import logging

import tweepy

from tweeter_crawler.crawler.authenticator import Authenticator
from tweeter_crawler.crawler.mysql_manager import MySqlManager

logging.basicConfig(filename='c:/tmp/example.log',level=logging.DEBUG)

class TwitterGetter(object):
    def __init__(self):
        self._api = tweepy.API(Authenticator.get_authentication_handler())
        self._mysql_manager = MySqlManager('twitter_influencers')

    def crawl(self, user):

        finished = False

        influencers_found = 0

        while not finished:
            try:
                potential_influencer_id, potential_influencer_details = self.get_user(user)
                if potential_influencer_id and self.is_influencer(potential_influencer_details) and not self._mysql_manager.influencer_parsed(potential_influencer_id):
                    potentials = self._api.friends_ids(potential_influencer_id)
                    influencer_friends = self.get_influencers_from_potentials(potentials)

                    for friend in influencer_friends:
                        self._mysql_manager.insert_influencer_to_table(friend)

                    self._mysql_manager.insert_influencer_to_table(self.make_inf_tuple(potential_influencer_id, potential_influencer_details, 1))
                    influencers_found += 1
                    user = self._mysql_manager.get_influencer_to_parse()
                    time.sleep(2)
                if influencers_found > 1000 or potential_influencer_id is None: finished = True

            except tweepy.RateLimitError:
                self._handle_rate_limit()
                user = self._mysql_manager.get_influencer_to_parse()
            except:
                logging.warn("could not put user: %s in table!", potential_influencer_id)
                user = self._mysql_manager.get_influencer_to_parse()

    def get_influencers_from_potentials(self, potentials):

        influencer_friends = []
        for potential in potentials:
            try:
                friend_id, friend_details = self.get_user(potential)
                if self.is_influencer(friend_details):
                    influencer_friends.append(self.make_inf_tuple(friend_id, friend_details, 0))
            except tweepy.RateLimitError:
                self._handle_rate_limit()
                continue
            except Exception:
                logging.warn("something went wront in get influencers from potentials for user: %s, proceesding to next user...", friend_id)
                continue

        return influencer_friends

    def make_inf_tuple(self, id,user_details, inf_or_potential):
        return (id,user_details.screen_name, user_details.name, user_details.location, user_details.description,
                user_details.followers_count, user_details.friends_count, user_details.statuses_count, user_details.url, inf_or_potential)

    def _handle_rate_limit(self):
        logging.info("Rate limit exceeded, waiting 15 minutes...")
        time.sleep(60 * 15)

    def get_user(self, user_id):
        if user_id:
            return user_id, self._api.get_user(user_id)
        else: return None, None

    # need more logic here -.-
    def is_influencer(self, user_details):
        return user_details.followers_count >= 2000

    def start_from(self):
        return self._mysql_manager.get_influencer_to_parse()


if __name__ == '__main__':
     twitter_getter = TwitterGetter()
     potential  = twitter_getter.start_from()
     if not potential:
         potential = 701983068 #GameGrumps
     twitter_getter.crawl(potential)