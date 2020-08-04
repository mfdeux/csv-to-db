from watchdog.events import PatternMatchingEventHandler
from watchdog.observers import Observer
import threading, time, signal

from datetime import timedelta

WAIT_TIME_SECONDS = 1

class EventHandler(PatternMatchingEventHandler):
    """
    EventHandler for file watcher - only watches files
    """

    def __init__(self, patterns: str = "*", ignore_patterns: str = None, ignore_directories: bool = True,
                 case_sensitive: bool = True, created: bool = True, modified: bool = True, deleted: bool = True):
        super().__init__(patterns, ignore_patterns, ignore_directories, case_sensitive)

        self.created = created
        self.deleted = deleted
        self.modified = modified

    def on_created(self, event):
        if self.created:
            print(f"hey, {event.src_path} has been created!")

    def on_modified(self, event):
        if self.modified:
            print(f"hey buddy, {event.src_path} has been modified")

    def on_deleted(self, event):
        if self.deleted:
            print(f"what the f**k! Someone deleted {event.src_path}!")


def make_observer(path: str = ".", recursive: bool = True, **kwargs):
    """
    Make observer

    """
    observer = Observer()
    event_handler = EventHandler(**kwargs)
    observer.schedule(event_handler, path, recursive=recursive)
    return observer


def make_signal_handlers():
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)


class ProgramKilled(Exception):
    pass


def foo():
    print
    time.ctime()


def signal_handler(signum, frame):
    raise ProgramKilled


class URLWatch(threading.Thread):
    def __init__(self, interval, execute, *args, **kwargs):
        threading.Thread.__init__(self)
        self.daemon = False
        self.stopped = threading.Event()
        self.interval = interval
        self.execute = execute
        self.args = args
        self.kwargs = kwargs

    def stop(self):
        self.stopped.set()
        self.join()

    def run(self):
        while not self.stopped.wait(self.interval.total_seconds()):
            self.execute(*self.args, **self.kwargs)

def make_url_watch(interval: int = 5):
    watch = URLWatch(interval)
    return watch

class GitRepoWatch(threading.Thread):
    def __init__(self, interval, execute, *args, **kwargs):
        threading.Thread.__init__(self)
        self.daemon = False
        self.stopped = threading.Event()
        self.interval = interval
        self.execute = execute
        self.args = args
        self.kwargs = kwargs

    def stop(self):
        self.stopped.set()
        self.join()

    def run(self):
        while not self.stopped.wait(self.interval.total_seconds()):
            self.execute(*self.args, **self.kwargs)