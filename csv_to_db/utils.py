import typing


def load_config(filepath: str) -> typing.Dict:
    """
    Load config file

    TODO: To come

    """
    pass


def snakecase(string: str) -> str:
    """
    Turns any string into a snakecase string
    """
    return string.lower().replace(' ', '_')
