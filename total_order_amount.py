from mrjob.job import MRJob

class TotalOrderAmount(MRJob):
    def mapper(self, _, line):
        costumer_id, _, price = line.split(',')
        yield costumer_id, float(price)

    def reducer(self, client, order_amounts):
        yield client, sum(order_amounts)

if __name__ == '__main__':
    TotalOrderAmount.run()