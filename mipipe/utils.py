def print_completion(func):
    def wrapper(*args, **kwargs):
        print(f"{func.__name__}...", end="")
        result = func(*args, **kwargs)
        print(f" Complete!")
        return result
    return wrapper
