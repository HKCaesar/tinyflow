class TinyFlowException(Exception):

    """Base exception for ``tinyflow``."""


class NotATransform(TinyFlowException):

    """Raise when an object should be an instance of
    ``tinyflow.transform.Transform()`` but isn't.
    """


def ensure_transform(obj):

    """Ensure an object is a ``tinyflow.transform.Transform()``.

    Parameters
    ----------
    obj : object
        Ensure this is the right type.

    Raises
    ------
    NotATransform

    Returns
    -------
    obj

    f"Expected a 'Transform()', not '{msg}'"
    """

    # Avoid a cyclic import
    from .transform import Transform

    if not isinstance(obj, Transform):
        raise NotATransform("Expected a 'Transform()', not '{}'".format(obj))
    else:
        return obj
