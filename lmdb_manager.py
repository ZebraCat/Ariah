import lmdb
import logging
import time

logging.basicConfig(filename='c:/tmp/example.log',level=logging.DEBUG)

class LmdbManager(object):

    def __init__(self, path_to_influencers, path_to_potentials):
        self._influencers_lmdb = lmdb.open(path_to_influencers, map_size=1024 * 1024 * 1024 * 5)
        self._potentials_lmdb = lmdb.open(path_to_potentials, map_size=1024 * 1024 * 1024 * 10)
        self._influencers = self._influencers_lmdb.begin(write=True)
        self._potentials = self._potentials_lmdb.begin(write=True)

    def close(self):
        self._influencers_lmdb.close()
        self._potentials_lmdb.close()

    def user_parsed(self, user_id):
        return self._influencers.get(str(user_id)) is not None

    def already_potential(self, potential):
        return self._potentials.get(str(potential)) is not None


    def next_potential(self):
        key = None
        cursor = self._potentials.cursor()
        for k,v in cursor:
            key = k
            self._potentials.pop(k)
            break
        if key is not None:
            self._potentials.commit()
        cursor.close()
        return key

    def move_to_influencers(self, influencer_id):
        cursor = self._influencers.cursor()
        cursor.put(str(influencer_id), 'influencer')
        cursor.close()

    def log_new_potentials(self, potentials):
        cursor = self._potentials.cursor()
        for potential in potentials:
            if not self.already_potential(potential) and not self.user_parsed(potential):
                cursor.put(str(potential), 'potential')

        cursor.close()

    def print_potentials(self):
        cursor = self._influencers.cursor()
        for k,v in cursor:
            print k,v

if __name__ == '__main__':
    m = LmdbManager('c:/tmp/influencers', 'c:/tmp/potentials')
    #time.sleep(3)
    #m.log_new_potentials([1,2,3])
    #k =  m.next_potential()
    #print k
    #m.move_to_influencers(k)
    # print m.already_potential(2)
    # print m.already_potential(3)
    # print m.already_potential(1)
    # print m.user_parsed(1)
    # print m.user_parsed(2)
    # print m.user_parsed(3)
    m.print_potentials()
    m.close()

