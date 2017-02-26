"""Exceptions, error handlers, and high level validators."""


class TinyFlowException(Exception):

    """Base exception for ``tinyflow``."""


class NotAnOperation(TinyFlowException):

    """Raise when an object should be an instance of
    ``tinyflow.ops.Operation()`` but isn't.
    """
