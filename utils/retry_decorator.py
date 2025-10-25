import time
import random

def retry(max_attempts=3, base_delay=0.1):
    def deco(func):
        def wrap(*args, **kwargs):
            for i in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    wait = base_delay * (2 ** i) + random.uniform(0, 0.1)
                    print(f"[retry] {func.__name__} failed: {e} | retrying in {wait:.2f}s")
                    time.sleep(wait)
            print(f"[error] {func.__name__} failed after {max_attempts} tries")
        return wrap
    return deco
