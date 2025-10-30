class SimpleCache:
    def __init__(self, max_size):
        self.cache = []
        self.max_size = max_size

    def get(self, index):
        """
        Retrieves an item from the cache by its index.
        Raises IndexError if the index is out of bounds.
        """
        if 0 <= index < len(self.cache):
            return self.cache[index]
        else:
            raise IndexError("Cache index out of range")

    def set(self, value):
        """
        Adds a value to the cache. If the cache is full,
        the oldest item is removed to make space.
        """
        if len(self.cache) >= self.max_size:
            # Remove the oldest item (first element)
            self.cache.pop(0)
        self.cache.append(value)

    def clear(self):
        """
        Clears all items from the cache.
        """
        self.cache.clear()

# Example Usage demonstrating FIFO behavior
print("--- Demonstrating FIFO Cache ---")
cache = SimpleCache(max_size=2)
print(f"Initial cache: {cache.cache}")

cache.set(1)
print(f"After setting 1: {cache.cache}") # Expected: [1]

cache.set(2)
print(f"After setting 2: {cache.cache}") # Expected: [1, 2]

cache.set(3) # Cache is full, 1 should be removed
print(f"After setting 3: {cache.cache}") # Expected: [2, 3]

cache.set(4) # Cache is full, 2 should be removed
print(f"After setting 4: {cache.cache}") # Expected: [3, 4]