class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        """
        Call the class and return the instance if it exists, otherwise create a new instance and return it.

        Parameters:
            cls (type): The class object.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            object: The instance of the class.
        """
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class Singleton(metaclass=SingletonMeta):
    pass
