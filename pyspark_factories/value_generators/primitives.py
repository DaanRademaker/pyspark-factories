import random


def create_random_boolean() -> bool:
    """Generates a random boolean value"""
    return bool(random.getrandbits(1))
