# classes/task_manager.py
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime


from classes.logger import Logger
logger = Logger().get_logger()


# task manager is implemented as a singleton, to make sure there is a single centralized task management functionality available across the entire project/

class TaskManager:
    _instance = None  # Singleton instance

    def __new__(cls, max_threads=2):                # 6 threads for normal operation.
        if cls._instance is None:
            cls._instance = super(TaskManager, cls).__new__(cls)
            cls._instance.executor = ThreadPoolExecutor(max_threads)
            cls._instance.futures = []              # computations that have not yet completed
            cls._instance.max_threads = max_threads
            logger.info(f"Task Manager initialized with {max_threads} threads.")
        return cls._instance


    def submit(self, fn, *args, **kwargs):
        """
        submit a task to the thread pool and track its Future.
        completed tasks are cleaned up from the task list.
        """
        # Cleanup completed Futures before adding a new one
        self._cleanup_futures()

        future = self.executor.submit(fn, *args, **kwargs)
        self.futures.append(future)
        return future


    def submit_untracked(self, fn, *args, **kwargs):
        """
        submit a task to the thread pool without tracking its Future.
        """
        self.executor.submit(fn, *args, **kwargs)


    def wait_for_all(self):
        """
        Wait for all tracked tasks to complete.
        """
        print("Waiting for all tasks to complete...")
        for future in as_completed(self.futures):
            try:
                result = future.result()
                logger.info(f"[{datetime.datetime.now()}] Task completed with result: {result}") # Wait for task completion
            except Exception as e:
                logger.error(f"Task failed with error: {e}")

        # Clean up all Futures after completion
        self._cleanup_futures()
        logger.info("All tasks completed.")


    def set_max_threads(self, max_threads):
        """
        Set the number of threads for the thread pool.
        """
        if not self.futures:  # Ensure no tasks are in progress
            self.executor = ThreadPoolExecutor(max_threads)
            self.max_threads = max_threads
        else:
            logger.error("Cannot change thread count while tasks are running.")
            raise RuntimeError("Cannot change thread count while tasks are running.")


    def reset(self, max_threads):
        """
        set a new maximum thread count.
        """
        if self.executor is not None:
            self.executor.shutdown(wait=True)
        self.executor = ThreadPoolExecutor(max_threads)
        self.max_threads = max_threads
        logger.info(f"Task Manager reset with {max_threads} threads.")


    def _cleanup_futures(self):
        """
        remove completed futures from the list.
        """
        self.futures = [f for f in self.futures if not f.done()]


    @classmethod
    def get_taskmgr(cls, max_threads=2):
        """
        set or retrieve the task manager instance.
        """
        if cls._instance is None:
            cls(max_threads=max_threads)
        return cls._instance

