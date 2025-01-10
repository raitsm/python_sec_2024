# project_root/decorators.py
# common module to place all the decorators in.
import datetime

def requires_loaded_data(method):
    """
    ensures the data is loaded into a dataset before executing a method.
    raises a ValueError if the data is not loaded.
    """
    def wrapper(self, *args, **kwargs):
        if not self.is_loaded:
            raise ValueError(f"Data not loaded into dataset '{self.dataset_config.get_id()}'.")
        return method(self, *args, **kwargs)
    return wrapper



def log_method_call(method):
    """
    Decorator to log the start and end time of a method call,
    along with the method name.
    """
    def wrapper(self, *args, **kwargs):
        start_time = datetime.datetime.now()
        print(f"[{start_time}] Starting method: {method.__name__}")
        
        result = method(self, *args, **kwargs)
        
        end_time = datetime.datetime.now()
        print(f"[{end_time}] Finished method: {method.__name__}")
        
        return result
    return wrapper