"""Exceptions, error handlers, and high level validators."""


class TinyFlowException(Exception):

    """Base exception for ``tinyflow``."""


class NotAnOperation(TinyFlowException):

    """Raise when an object should be an instance of
    ``tinyflow.ops.Operation()`` but isn't.
    """


class NotACoroOperation(NotAnOperation):

    """Like ``NotAnOperation()`` but for ``tinyflow.coro.ops``."""


class NotACoroTarget(NotACoroOperation):

    """Like ``NotACoroOperation()`` but for coroutine targets."""


class TooManyTargets(TinyFlowException):

    """Raised when a ``tinyflow.coro.CoroPipeline()`` receives too many
    ``tinyflow.coro.ops.CoroTarget()``'s.
    """


class NoPipeline(TinyFlowException):

    """Raised when an operation has not been attached to a pipeline, but
    requests its parent pipeline.
    """


class NoPool(TinyFlowException):

    """Raised when a thread or process pool is requested but was not passed
    to ``tinyflow.Pipeline()``.
    """
