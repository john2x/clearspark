from rq import Queue
from worker import conn
q = Queue(connection=conn)

class Queue:
    def _has_completed(queue_name, domain=False):
        ''' '''
        if domain:
            jobs = [job for job in q.jobs if "domain" in job.meta.keys()]
            jobs = [job for job in jobs if job.meta["domain"] ==  domain]
        else:
            jobs = [job for job in q.jobs if queue_name in job.meta.keys()]

        return len(jobs) == 0

    def _results(queue_name, domain=False):
        if domain:
            jobs = [job for job in q.jobs if "domain" in job.meta.keys()]
            jobs = [job for job in jobs if job.meta["domain"] ==  domain]
        else:
            jobs = [job for job in q.jobs if queue_name in job.meta.keys()]
        
        return [job.results for job in jobs]
