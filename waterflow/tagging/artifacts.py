import pandas as pd

from waterflow.config import OBJECTS, EXP_OBJECTS


class Tags(object):

    def __init__(self):
        self.queue = {}

        # todo: store custom vars in cust_queue for type handling
        #  and nested values
        cust_queue = {}

    def save(self, artifact, obj: str, dtype: type =None):
        """
        Tags a variable as something to be saved

        Parameters
        ----------
        artifact: variable to store
        obj: str. type of variable to be store.
            Choice of config.OBJECTS
            If you pass a string not in config.OBJECTS
            WaterFlow will store the object under the cust dir
        dtype: type or None, Default None
            used for storing custom variables not in OBJECTS
        """

        if obj in OBJECTS:
            self.queue[obj] = artifact
        elif not dtype:
            raise ValueError("dtype must be provided if custom obj")
        else:
            self.queue['cust' + ':' + obj + ':' + dtype.__name__] = artifact

        return artifact

    def ret_queue(self) -> dict:
        return self.queue

    # todo return EXP_OBJECTS and CUSTOM
    # todo add varaible type. Maybe shape for pd dataframe
    # todo col for varaible name. %who, local(), dir()
    def inspect(self):
        """
        Returns pd.Dataframe of tagged variables and their values.
        Missing variables will have `NaN` value
        """
        artifact = [self.queue.get(artif) for artif in EXP_OBJECTS]
        return pd.DataFrame({'artifact': artifact}, index=EXP_OBJECTS)

    def flush(self, proj, exp, tag):
        """
        Pushes all variables from `queue` to metadata store

        Parameters
        ----------
        proj: project name on metadata provider
        exp: experiment name
        tag: custom commit message

        :return:
        """
        # call out s3 service
        # todo create metadata provider file to hook into s3 and blob
        # todo get columns and types of pandas dfs

