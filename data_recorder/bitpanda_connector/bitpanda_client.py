from data_recorder.connector_components.client import Client


class BitpandaClient(Client):

    def __init__(self, ccy):
        super(BitpandaClient, self).__init__(ccy, 'bitpanda')

    def run(self):
        """
        Handle incoming level 3 data on a separate thread
        :return: (void)
        """
        while True:
            msg = self.queue.get()

            if self.book.new_tick(msg) is False:
                self.book.load_book()
                self.retry_counter += 1
                print('\n[Bitpanda - %s] ...going to try and reload the order '
                      'book\n' % self.sym)
                continue
