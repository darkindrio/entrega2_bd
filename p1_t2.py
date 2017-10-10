from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol


class MRWordFrequencyCount(MRJob):
        INPUT_PROTOCOL = JSONValueProtocol


        def mapper(self, _, line):
		review_id = line['review_id']
		words = line['text'].split()
                for word in words:
                	yield [word.lower(),review_id], 1


	def reducer_total_words(self, key, count):
		yield key, sum(count)

        def reducer_count_words(self, word_with_rev, count):
		#aux = count.copy()
		aux = list(count)
		#print word_with_rev
		if int(aux[0]) < 2:
			yield word_with_rev[1], 1
		
	def reducer_agroup_words_by_rev(self, rev_id, count):
		yield 'max', [sum(count), rev_id]

	def reducer_max_word(self, rev_id, count):
		yield rev_id, max(count)

        def steps(self):
                return [
                        MRStep(mapper=self.mapper,
                        reducer=self.reducer_total_words), MRStep(reducer = self.reducer_count_words), 
			MRStep(reducer = self.reducer_agroup_words_by_rev),
			MRStep(reducer = self.reducer_max_word)
                         ]


if __name__ == '__main__':
    MRWordFrequencyCount.run()

