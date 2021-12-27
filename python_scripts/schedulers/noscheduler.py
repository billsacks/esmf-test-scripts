from schedulers.scheduler import Scheduler


class NoScheduler(Scheduler):
    TEMPLATE_PATH = None
    SCHEDULER_TYPE = "None"

    def checkqueue(self, _):
        return True
