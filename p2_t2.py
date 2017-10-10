from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import re
import itertools
import csv

 
class UsersCount(MRJob):

    INPUT_PROTOCOL = JSONValueProtocol

    def mapper_file(self, _, line):
	review_id = line['business_id']
	user_id = line['user_id']
        yield user_id, review_id 
 
    def reducer_users_id(self, user_id, movies_ids):
	aux_gen = list(movies_ids)
	total_movies = len(aux_gen)
	for movie_id in aux_gen:
		yield movie_id, [user_id, total_movies]
	 
    def reducer(self, movie_id, users_id):
	temp_list = list(users_id)
	yield [movie_id, temp_list]
 
    def mapper_set_users(self,movie_id, users_ids_extended):
	if len(users_ids_extended) > 1:
		for pair_of_users in itertools.combinations(users_ids_extended, 2):
			yield  [pair_of_users, 1]
 
    def reducer2(self, key, values):
	user1_total = key[0][1]
	user2_total = key[1][1]
	total = user1_total + user2_total
	if float(sum(values))/float(total) > 0.5:
        	yield ['MAX',[float(sum(values))/float(total)]]
	 
    def max_reducer(self, stat, values):
        yield [stat,max(values)]
 
    def steps(self):
	 return [MRStep(mapper=self.mapper_file, reducer = self.reducer_users_id),
                MRStep(reducer = self.reducer) ,
                MRStep(mapper = self.mapper_set_users, reducer = self.reducer2),
                MRStep(reducer = self.max_reducer)]


 
 
if __name__ == '__main__':
    UsersCount.run()
