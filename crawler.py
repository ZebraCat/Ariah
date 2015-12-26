import time
import logging

import tweepy
from tweepy import StreamListener, Stream

from tweeter_crawler.crawler.authenticator import Authenticator
from tweeter_crawler.crawler.lmdb_manager import LmdbManager
from tweeter_crawler.crawler.thrift_hbase.hbase_writer import HBaseWriter

logging.basicConfig(filename='c:/tmp/example.log',level=logging.DEBUG)

class TwitterGetter(object):
    def __init__(self):
        self._api = tweepy.API(Authenticator.get_authentication_handler())
        self._writer = HBaseWriter('Influencers')
        self._lmdb_manager = LmdbManager('c:/tmp/influencers', 'c:/tmp/potentials')

    def put_user_in_table(self, user_id, user_details):
        key = str(user_id)
        details = "{}:{}:{}:{}:{}:{}:{}:{}".format(user_details.screen_name, user_details.name, user_details.location, user_details.description,
                                                   user_details.followers_count,user_details.friends_count,
                                                   user_details.statuses_count, user_details.url)

        logging.info("User: %s details: %s", key, details)
        self._writer.put(unicode(user_id), 'details', 'details', unicode(details))


    def crawl(self, user):

        finished = False

        influencers_found = 0

        while not finished:
            try:
                potential_influencer_id, potential_influencer_details = self.get_user(user)
                if potential_influencer_id and self.is_influencer(potential_influencer_details) and not self._lmdb_manager.user_parsed(potential_influencer_id):
                    self._lmdb_manager.move_to_influencers(potential_influencer_id)
                    potentials = self._api.friends_ids(potential_influencer_id)
                    self._lmdb_manager.log_new_potentials(potentials)
                    self.put_user_in_table(potential_influencer_id, potential_influencer_details)
                    influencers_found += 1
                    user = self._lmdb_manager.next_potential()
                    time.sleep(10)
                if influencers_found > 1000 or potential_influencer_id is None: finished = True

            except tweepy.RateLimitError:
                self._handle_rate_limit()
            except:
                logging.warn("could not put user: %s in table!", potential_influencer_id)

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
        return self._lmdb_manager.next_potential()


if __name__ == '__main__':
    twitter_getter = TwitterGetter()
    potential  = twitter_getter.start_from()
    if not potential:
        potential = 'GameGrumps'
    twitter_getter.crawl(potential)