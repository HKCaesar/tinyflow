"""Pipeline model."""


from .errors import ensure_transform


class Pipeline(object):

    """A ``tinyflow`` pipeline model."""

    def __init__(self):
        self.transforms = []

    def __or__(self, other):

        """Add a transform to the pipeline."""

        self.transforms.append(ensure_transform(other))
        return self

    __ior__ = __or__

    def __call__(self, data):

        """Stream data through the pipeline."""

        data = iter(data)

        for trans in self.transforms:
            data = trans(data)
        yield from data
