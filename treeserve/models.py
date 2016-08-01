from base64 import b64decode
from time import time
from grp import getgrgid
from pickle import dumps, loads
from pwd import getpwuid
from typing import List


class Node:
    """
    An object on the filesystem.
    """
    _user_name_cache = {}
    _group_name_cache = {}

    def __init__(self, tokens: List[str], txn):
        now = time()  # Time in seconds since the epoch
        seconds_in_year = 60 * 60 * 24 * 365
        one_tib = 1024 ** 4
        cost_per_tib_year = 150

        # Self-documenting, woo!
        path = b64decode(tokens[0]).decode()
        size = int(tokens[1])
        uid = int(tokens[2])
        gid = int(tokens[3])
        atime = tokens[4]
        mtime = tokens[5]
        ctime = tokens[6]
        mode = tokens[7]
        inode = tokens[8]
        hardlinks = tokens[9]
        device_id = tokens[10]

        # Test for existing stub (with child_dirs != []).
        fs_object = txn.get(path.encode())
        if fs_object is None:
            fs_object = {}

        fs_object.update({
            "data": {
                "size": size,
                # time (secs) since created/modified/accessed
                # ------------------------------------------- * cost per TiB-year * size in TiB
                #               seconds in year
                "ctime_cost": (now-int(ctime) / seconds_in_year) * cost_per_tib_year * (size / one_tib),
                "mtime_cost": (now-int(mtime) / seconds_in_year) * cost_per_tib_year * (size / one_tib),
                "atime_cost": (now-int(atime) / seconds_in_year) * cost_per_tib_year * (size / one_tib)
            },
            "user": self._get_user_name(uid),
            "group": self._get_group_name(gid),
            "mode": mode,
            # These aren't exposed in the API, so we don't need to store them
            # "inode_number": inode,
            # "hard_links": hardlinks,
            # "device_id": device_id
        })

        txn.put(path.encode(), dumps(fs_object))

        # Update parent node with information about this node.
        # NB: every directory node with contents should have a child_dirs key, because
        # every directory with files (directories don't count as files - see /lustre, depth 0 and
        # compare with /lustre/scratch115, depth 0) will have a virtual child "*.*" representing
        # the values for the files in that directory.
        split_path = path.split("/")
        parent_path = "/".join(split_path[:len(split_path)-1])
        try:
            parent = loads(txn.get(parent_path.encode()))
        except TypeError:
            # No parent node, create a stub.
            parent = {
                "child_dirs": []
            }
        if mode == "d":
            # We're a directory, tell the parent about ourselves.
            try:
                parent["child_dirs"].append(path)
            except KeyError:
                parent["child_dirs"] = [path]
        else:
            # We're a file/link/something else, add our size etc. to the parent's totals.
            # This is deferred until the tree is being finalized.
            pass
            # try:
            #     parent["child_dirs"]["*.*"] = self.update(parent["child_dirs"]["*.*"], fs_object)
            # except KeyError:
            #     parent["child_dirs"]["*.*"] = fs_object
        txn.put(parent_path.encode(), dumps(parent))

    @classmethod
    def _get_user_name(cls, uid: int) -> str:
        """
        Get the human-readable username associated with a user ID if possible; otherwise, return
        the user ID.
        :param uid: a numeric user ID
        :return: a human-readable username
        """
        user_name = None
        if uid in cls._user_name_cache:
            user_name = cls._user_name_cache[uid]
        else:
            try:
                user_name = getpwuid(uid)[0]
            except KeyError:
                pass
            else:
                cls._user_name_cache[uid] = user_name
        return str(user_name if user_name is not None else uid)

    @classmethod
    def _get_group_name(cls, gid: int) -> str:
        """
        Get the human-readable group name associated with a group ID if possible; otherwise, return
        the group ID.
        :param gid: a numeric group ID
        :return: a human-readable group name
        """
        group_name = None
        if gid in cls._group_name_cache:
            group_name = cls._group_name_cache[gid]
        else:
            try:
                group_name = getgrgid(gid)[0]
            except KeyError:
                pass
            else:
                cls._group_name_cache[gid] = group_name
        return str(group_name if group_name is not None else gid)
