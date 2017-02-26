"""Exceptions, error handlers, and high level validators."""


class TinyFlowException(Exception):

    """Base exception for ``tinyflow``."""


class NotAnOperation(TinyFlowException):

    """Raise when an object should be an instance of
    ``tinyflow.ops.Operation()`` but isn't.
    """


def ensure_operation(obj):

    """Ensure an object is a ``tinyflow.ops.Operation()``.

    Parameters
    ----------
    obj : object
        Ensure this is the right type.

    Raises
    ------
    NotAOperation

    Returns
    -------
    obj
    """

    # Avoid a cyclic import
    from .serial.ops import Operation

    if not isinstance(obj, Operation):
        raise NotAnOperation("Expected a 'Operation()', not '{}'".format(obj))
    else:
        return obj
