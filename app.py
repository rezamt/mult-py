from queue import Queue
from threading import Thread

cfn_status = []


class Worker(Thread):
    """Thread executing tasks from a given tasks queue"""
    def __init__(self, tasks):
        Thread.__init__(self)
        self.tasks = tasks
        self.daemon = True
        self.start()

    def run(self):
        while True:
            func, args, kargs = self.tasks.get()
            try:
                func(*args, **kargs)

            except Exception as e:
                print(e)
            finally:
                self.tasks.task_done()


class ThreadPool:
    """Pool of threads consuming tasks from a queue"""
    def __init__(self, num_threads):
        self.tasks = Queue(num_threads)
        for _ in range(num_threads):
            Worker(self.tasks)

    def add_task(self, func, *args, **kargs):
        """Add a task to the queue"""
        self.tasks.put((func, args, kargs))

    def wait_completion(self):
        """Wait for completion of all the tasks in the queue"""
        self.tasks.join()

if __name__ == '__main__':

    THREAD_POOL_SIZE=10

    from random import randrange
    from time import sleep

    delays = [randrange(1, 20) for i in range(20)] # Number of Account and Randome Processing time for them

    def callback(account: str, status: dict):
        cfn_status.append({account: status})

    def deploy_cfn(account, wait_time, callback):
        print('CFN deployment for Account {} with fake processing time: {}'.format(account, wait_time))
        sleep(wait_time)
        callback(account, {'status' : 'Template Deployed after {}'.format(wait_time)})

    pool = ThreadPool(THREAD_POOL_SIZE)
    result = {}
    for i, wait_time in enumerate(delays):
        pool.add_task(deploy_cfn, 'AWS_ACCOUNT_{}'.format(i), wait_time, callback)

    pool.wait_completion()

    for i,j in enumerate(cfn_status):
        print(j)