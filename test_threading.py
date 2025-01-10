import threading
import time

lock = threading.Lock()

def critical_section(thread_id):
    print(f"Thread {thread_id} attempting to acquire lock.")
    with lock:  # Acquire the lock
        print(f"Thread {thread_id} has acquired the lock.")
        time.sleep(2)  # Simulate a time-consuming task
        print(f"Thread {thread_id} releasing the lock.")

# Create two threads
thread1 = threading.Thread(target=critical_section, args=(1,))
thread2 = threading.Thread(target=critical_section, args=(2,))

thread1.start()
thread2.start()

thread1.join()
thread2.join()
