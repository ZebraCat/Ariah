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


    def crawl(self):

        finished = False

        influencers_found = 0

        while not finished:
            try:
                potential_influencer_id, potential_influencer_details = self.get_user(self._lmdb_manager.next_potential())
                if potential_influencer_id and self.is_influencer(potential_influencer_details) and not self._lmdb_manager.user_parsed(potential_influencer_id):
                    self._lmdb_manager.move_to_influencers(potential_influencer_id)
                    potentials = self.get_contacts_ids(potential_influencer_id, 'friends')
                    self._lmdb_manager.log_new_potentials(potentials)
                    self.put_user_in_table(potential_influencer_id, potential_influencer_details)
                    influencers_found += 1

                if influencers_found > 1000 or potential_influencer_id is None: finished = True

            except tweepy.RateLimitError:
                self._handle_rate_limit()
            except:
                logging.warn("could not put user: %s in table!", potential_influencer_id)

    def create_initial_influencer(self, initial):
        self._lmdb_manager.log_new_potentials([initial])

    def _handle_rate_limit(self):
        logging.info("Rate limit exceeded, waiting 15 minutes...")
        time.sleep(60 * 15)

    def get_contacts_ids(self, user_id, ids='followers'):
        contacts = []
        ids = self._api.followers_ids if ids == 'followers' else self._api.friends_ids
        for page in tweepy.Cursor(ids, screen_name=user_id).pages():
            contacts.extend(page)
            logging.info("contacts current length for user %s is: %s",user_id ,(len(contacts)))
            if len(contacts) > 5000:
                break
            time.sleep(10)
        return contacts

    def get_user(self, user_id):
        return user_id, self._api.get_user(user_id)

    # need more logic here -.-
    def is_influencer(self, user_details):
        return 10000 <= user_details.followers_count <= 50000



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
    twitter_getter = TwitterGetter()
    twitter_getter.create_initial_influencer('therealguypines')
    twitter_getter.crawl()
    #TopicListener(['Indiana', 'weather']).execute()
    #TopicListener([-86.33,41.63,-86.20,41.74]).execute(True)