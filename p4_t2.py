from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import re
import itertools
import csv
import math
 
class UsersCount(MRJob):

    INPUT_PROTOCOL = JSONValueProtocol
    def mapper_file(self, _, line):
	review_id = line['business_id']
	user_id = line['user_id']
	star = line['stars']
        yield user_id ,[star, review_id] 

 
    def reducer_users_id(self, user_id, movies_ids):
	total = 1
	aux_gen = list(movies_ids)
	for x in aux_gen:
		total = (int(x[0])/5)*(int(x[0])/5)*total
	for movie_id in aux_gen:
		yield movie_id[1], [user_id, total, movie_id[0]]
	 
    def reducer(self, movie_id, users_id):
	temp_list = list(users_id)
	yield [movie_id, temp_list]
 
    def mapper_set_users(self,movie_id, users_ids_extended):	
	if len(users_ids_extended) > 1:
		for pair_of_users in itertools.combinations(users_ids_extended, 2):
			rev1 = pair_of_users[0][2]
			rev2 = pair_of_users[1][2]
			yield pair_of_users, (rev1/5)*(rev2/5)
 
    def reducer2(self, key, values):
	user1_total = math.sqrt(key[0][1])
        user2_total = math.sqrt(key[1][1])
	if user1_total !=0 and user2_total != 0:
		lower = user1_total * user2_total
        	yield ['max' ,float(sum(values))/float(lower)]
	 
    def max_reducer(self, stat, values):
        TEMP = [values]
        yield [stat,max(values)]
 
    def steps(self):
	return [MRStep(mapper=self.mapper_file, reducer = self.reducer_users_id),
		MRStep(reducer = self.reducer),
		MRStep(mapper = self.mapper_set_users, reducer = self.reducer2),
                MRStep(reducer = self.max_reducer)]


 
 
if __name__ == '__main__':
    UsersCount.run()
