
import ConfigParser
import tweepy

parser = ConfigParser.SafeConfigParser()

class Authenticator(object):

    _auth = None

    @classmethod
    def get_authentication_handler(cls):
        if not cls._auth:
            parser.read('../dev-props.cnf')
            print parser.sections()
            consumer_key = parser.get('Consumer', 'consumer_key')
            consumer_secret = parser.get('Consumer', 'consumer_secret')
            access_token = parser.get('TwitterAccessToken', 'access_token')
            access_token_secret = parser.get('TwitterAccessToken', 'access_token_secret')

            auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
            auth.set_access_token(access_token, access_token_secret)
            cls._auth = auth

        return cls._auth