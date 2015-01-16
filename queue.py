from rq import Queue
from worker import conn
q = Queue(connection=conn)

class RQueue:
    def _has_completed(self, queue_name, domain=False):
        print "RQUEUE HAS COMPLETED CHECK"
        if domain:
            jobs = [job for job in q.jobs if "domain" in job.meta.keys()]
            jobs = [job for job in jobs if job.meta["domain"] ==  domain]
        else:
            jobs = [job for job in q.jobs if queue_name in job.meta.keys()]
        print len(jobs), " <-- queue jobs"
        return len(jobs) == 0

    def _meta(self, job, queue_name, value=True):
        job.meta[queue_name] = value
        job.save()

    def _results(queue_name, domain=False):
        if domain:
            jobs = [job for job in q.jobs if "domain" in job.meta.keys()]
            jobs = [job for job in jobs if job.meta["domain"] ==  domain]
        else:
            jobs = [job for job in q.jobs if queue_name in job.meta.keys()]
        
        return [job.results for job in jobs]
