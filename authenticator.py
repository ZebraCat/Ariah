
import ConfigParser
import tweepy

parser = ConfigParser.SafeConfigParser()

class Authenticator(object):

    _auth = None

    @classmethod
    def get_authentication_handler(cls):
        if not cls._auth:
            parser.read('dev-props.cnf')

            consumer_key = parser.get('Consumer', 'consumer_key')
            consumer_secret = parser.get('Consumer', 'consumer_secret')
            access_token = parser.get('AccessToken', 'access_token')
            access_token_secret = parser.get('AccessToken', 'access_token_secret')

            auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
            auth.set_access_token(access_token, access_token_secret)
            _auth = auth

        return _auth