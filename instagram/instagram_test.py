from instagram.client import InstagramAPI

access_token = '2395051068.3adebb9.d57fa5cdd36549048e20c9352a77864d'
client_secret = 'cbfe9fc9a9414145a68befa1b2e76ed6'

api = InstagramAPI(access_token= access_token, client_secret=client_secret)
user = api.user(1574083)

print user.id
