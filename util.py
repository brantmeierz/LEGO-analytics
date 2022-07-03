def money_format(float_value: float) -> str:
    """
    Format a money value as a proper string.

    :param float_value: The value to format.
    :type: float
    :return: The value prefixed with $ and showing two decimal places.
    :rtype: str
    """
    return "$" + str('{0:.2f}'.format(float_value))