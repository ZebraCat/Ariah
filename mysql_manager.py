import pymysql

class MySqlManager(object):

    # TODO - create orm classes?
    COLUMNS = 'inf_id, inf_screen_name, inf_name, inf_location, inf_description, inf_followers_count, inf_friends_count, inf_statuses_count, inf_url, done_parsing_influencer'

    def __init__(self, table_name):
        self.conn = pymysql.connect(host='127.0.0.1', port=3307, user='root', passwd='root', db='influencers')
        self.table_name = table_name

    def insert_influencer_to_table(self, values):
        curr = self.conn.cursor()
        curr.execute("REPLACE INTO {}({}) VALUES(%s,%s,%s,%s,%s,%s,%s,%s, %s, %s)".format(self.table_name, self.COLUMNS), values)
        self.conn.commit()

    def done_parsing_influencer(self, inf_id):
        curr = self.conn.cursor()
        curr.execute("UPDATE {} SET done_parsing_influencer=1 WHERE inf_id={}".format(self.table_name, inf_id))
        self.conn.commit()

    def get_influencer_to_parse(self):
        curr = self.conn.cursor()
        res = curr.execute("SELECT inf_id FROM {} WHERE done_parsing_influencer=0 LIMIT 1".format(self.table_name))
        return None if not res else curr.fetchone()[0]

    def influencer_parsed(self, inf_id):
        curr = self.conn.cursor()
        res = curr.execute("SELECT done_parsing_influencer FROM {} WHERE inf_id={}".format(self.table_name, inf_id))
        return False if not res or curr.fetchone()[0] == 0 else True



if __name__ == '__main__':
    sql = MySqlManager('twitter_influencers')
    print sql.get_influencer_to_parse()