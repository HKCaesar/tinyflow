"""Pipeline model."""


from .exceptions import NotAnOperation
from .ops import Operation


class Pipeline(object):

    """A ``tinyflow`` pipeline model."""

    def __init__(self):
        self.transforms = []

    def __or__(self, other):

        """Add a transform to the pipeline."""

        if not isinstance(other, Operation):
            raise NotAnOperation(f"Expected an 'Operation()', not: {other}")
        self.transforms.append(other)
        return self

    __ior__ = __or__

    def __call__(self, data):

        """Stream data through the pipeline."""

        data = iter(data)

        for trans in self.transforms:
            # Ensure downstream nodes get an ambiguous iterator and not
            # something like a list that they get hooked on abusing.
            data = iter(trans(data))
        for item in data:
            yield item
