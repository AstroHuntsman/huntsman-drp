"""
See: https://github.com/lsst/pipe_tasks/blob/master/python/lsst/pipe/tasks/makeDiscreteSkyMap.py
"""
from collections import defaultdict


def get_patch_ids(skymap):
    """
    """
    indices = defaultdict(list)
    for tract_info in skymap:

        # Identify the tract
        tract_id = tract_info.getId()

        # Get lists of x-y patch indices in this tract
        nx = tract_info.getNumPatches()[0]
        ny = tract_info.getNumPatches()[1]
        for x in range(nx):
            for y in range(ny):
                indices[tract_id].append(f"{x},{y}")

    return indices
